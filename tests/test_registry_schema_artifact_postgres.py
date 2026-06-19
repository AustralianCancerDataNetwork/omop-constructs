from __future__ import annotations

import difflib
import importlib
import os
import shutil
import sys
from pathlib import Path

import pytest
from orm_loader.helpers import Base

from omop_constructs import get_complete_construct_registry
from omop_constructs.core.schema_snapshot import write_registry_schema_snapshot


ARTIFACT_PATH = Path(__file__).parent / "artifacts" / "construct_registry_schema.csv"


def _clear_construct_import_state() -> None:
    constructs = importlib.import_module("omop_constructs.core.constructs")
    registered = list(constructs._CONSTRUCTS.values())
    constructs._CONSTRUCTS.clear()

    for cls in registered:
        table = getattr(cls, "__table__", None)
        if table is not None and table.key in Base.metadata.tables:
            Base.metadata.remove(Base.metadata.tables[table.key])

    for module_name in list(sys.modules):
        if (
            module_name.startswith("omop_constructs.alchemy")
            or module_name.startswith("omop_constructs.semantics")
            or module_name in {"omop_constructs.bootstrap", "omop_constructs.bootstrap_modules"}
        ):
            sys.modules.pop(module_name, None)


@pytest.mark.postgres
def test_registry_schema_snapshot_matches_checked_in_artifact(
    pg_bootstrapped_engine,
    tmp_path,
):
    del pg_bootstrapped_engine  # fixture sets up isolated registry import environment

    _clear_construct_import_state()
    registry = get_complete_construct_registry()

    generated = tmp_path / "construct_registry_schema.generated.csv"
    write_registry_schema_snapshot(generated, registry=registry)

    if os.getenv("UPDATE_REGISTRY_SCHEMA_SNAPSHOT") == "1":
        ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(generated, ARTIFACT_PATH)
        return

    if not ARTIFACT_PATH.exists():
        pytest.fail(
            "Missing checked-in registry schema artifact. Generate it with:\n"
            "UPDATE_REGISTRY_SCHEMA_SNAPSHOT=1 "
            "uv run --extra dev python -m pytest "
            "tests/test_registry_schema_artifact_postgres.py -m postgres -q"
        )

    expected = ARTIFACT_PATH.read_text(encoding="utf-8")
    actual = generated.read_text(encoding="utf-8")

    if expected != actual:
        diff = "\n".join(
            difflib.unified_diff(
                expected.splitlines(),
                actual.splitlines(),
                fromfile=str(ARTIFACT_PATH),
                tofile=str(generated),
                lineterm="",
            )
        )
        pytest.fail(
            "Registry schema artifact is stale. Regenerate it with:\n"
            "UPDATE_REGISTRY_SCHEMA_SNAPSHOT=1 "
            "uv run --extra dev python -m pytest "
            "tests/test_registry_schema_artifact_postgres.py -m postgres -q\n\n"
            f"{diff}"
        )
