"""Treat SQLAlchemy mapper warnings as errors on a clean construct import.

OC-M4 recorded 190 ``SAWarning``s about classes being replaced in the
declarative registry. Those all come from the test harness: three tests clear
``sys.modules`` and re-import the construct modules against a live declarative
base, so every mapped class is registered twice. A single clean import of the
manifest emits none, which means the mappings themselves are conflict-free and
the warnings are a reload artefact rather than a mapping defect. OC-3 removes
the reload.

That distinction is what makes this test useful now: because a clean import is
already warning-free, asserting zero warnings cannot be satisfied by accident,
and a genuinely new identity conflict — two constructs claiming one table name,
a duplicated ``__mv_name__``, an unresolvable relationship — fails here
immediately instead of being buried in the existing warning noise.

Run in a subprocess deliberately. In-process the result would depend on
whatever the rest of the suite had already imported, which is precisely the
state that generates the OC-M4 warnings.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

#: Imports the full manifest, configures every mapper, and compiles every
#: construct with SAWarning promoted to an exception. Kept as source text
#: rather than a helper module so the child interpreter starts with nothing
#: from omop_constructs already imported.
CLEAN_IMPORT_PROGRAM = textwrap.dedent(
    """
    import sys
    import warnings

    from sqlalchemy.exc import SAWarning

    # Promote before importing anything from omop_constructs: mapper
    # registration happens at import time, so a filter installed afterwards
    # would miss exactly the warnings this test exists to catch.
    warnings.simplefilter("error", SAWarning)

    import sqlalchemy.orm as so

    from omop_constructs.bootstrap import get_complete_construct_registry

    registry = get_complete_construct_registry()
    so.configure_mappers()
    registry.compile_check()

    print(f"OK {len(registry.plan())}")
    """
)


@pytest.mark.postgres
def test_clean_construct_import_emits_no_mapper_warnings(pg_bootstrapped_engine):
    """Importing every construct once must not emit a single SAWarning.

    The fixture points ``OA_CONFIG_PATH``, ``ENGINE``, and ``ENGINE_CDM`` at the
    disposable scratch database, and the child process inherits them, so the
    resolver-backed imports resolve against the scratch CDM rather than the
    developer's configured one.
    """
    del pg_bootstrapped_engine  # needed only for the environment it sets

    result = subprocess.run(
        [sys.executable, "-c", CLEAN_IMPORT_PROGRAM],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    if result.returncode != 0:
        pytest.fail(
            "A clean construct import raised on a SQLAlchemy mapper warning, or "
            "failed outright. Either a new mapper identity conflict was "
            "introduced, or the registry no longer imports cleanly.\n\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    assert result.stdout.strip().startswith("OK "), result.stdout


@pytest.mark.postgres
def test_the_warning_filter_in_that_program_actually_bites(pg_bootstrapped_engine):
    """Guard the guard: prove the promoted filter fails on a real conflict.

    Without this, a change that stopped SAWarning from being promoted — a moved
    import, a reset filter — would leave the test above passing vacuously
    forever.
    """
    del pg_bootstrapped_engine

    program = CLEAN_IMPORT_PROGRAM + textwrap.dedent(
        """
        # Re-register one construct module against the live declarative base.
        # This is the same duplicate-registration that produces the OC-M4
        # warnings, so the promoted filter must turn it into an exception.
        import importlib

        module_name = "omop_constructs.alchemy.demography.demography_matview"
        sys.modules.pop(module_name, None)
        importlib.import_module(module_name)

        print("FILTER DID NOT BITE")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    assert result.returncode != 0, (
        "Re-registering a construct did not raise, so SAWarning is not being "
        f"promoted to an error.\n\nstdout:\n{result.stdout}"
    )
    assert "FILTER DID NOT BITE" not in result.stdout
    assert "SAWarning" in result.stderr, result.stderr
