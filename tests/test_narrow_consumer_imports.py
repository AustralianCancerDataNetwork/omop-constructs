"""Guard the package-level import surface that downstream packages actually use.

Every other test here reaches the constructs either through the full
``bootstrap_modules`` manifest or through synthetic classes. The manifest imports
every construct module explicitly, so it can never surface a construct that is
only reachable because some *other* module happens to import it. Downstream
packages do not use the manifest: they import a subset of names straight from
``omop_constructs.alchemy.*``, which resolve through the lazy export maps in each
subpackage's ``__init__``.

That difference is not theoretical. During the cava-devops adoption, ruff removed
an unused ``TreatmentEnvelopeMV`` import from ``treatment_summary_mv.py``. The
manifest-based tests stayed green because the manifest imports
``treatment_envelope_mv`` in its own right — they could not have detected a lost
incidental import either way. These tests cover that path instead.

They deliberately assert on the names oa-cohorts imports and the attributes it
uses, so that removing or renaming one fails here rather than in a downstream
repo.

Marked ``postgres`` because importing ``omop_constructs.alchemy.*`` builds an engine
and resolvers at module scope, so it needs the vocabulary *tables* to exist. It does
not need them populated: the assertions are about the import surface, not concept
content, and the scratch CDM the fixture bootstraps is empty. That keeps these tests
free of any vocabulary-version dependency.
"""

import importlib
import sys

import pytest
import sqlalchemy.orm as so
from orm_loader.helpers import Base

# Mirrors oa_cohorts/measurables/tx_measurables.py, dx_measurables.py,
# ev_measureables.py and pr_measurables.py. Keep in step with those.
CONSUMED_EXPORTS: dict[str, tuple[str, ...]] = {
    "omop_constructs.alchemy.episodes": (
        "ConditionTreatmentEpisode",
        "ConditionTreatmentIntentMV",
        "ConsultWindowMV",
        "DxTreatStartMV",
        "SurgicalProcedureMV",
        "TreatmentEnvelopeMV",
    ),
    "omop_constructs.alchemy.events": (
        "DxMeasurementMV",
        "DxProcedureMV",
        "DxObservationMV",
    ),
    "omop_constructs.alchemy.modifiers": (
        "ModifiedCondition",
    ),
    "omop_constructs.alchemy.demography": (
        "PersonDemography",
    ),
}

# Columns oa-cohorts' measurable specs name directly. A construct can import and
# map cleanly while still having lost the column a consumer reads.
CONSUMED_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("omop_constructs.alchemy.episodes", "TreatmentEnvelopeMV"): (
        "person_id",
        "condition_episode",
        "condition_start_date",
        "treatment_days_before_death",
    ),
}


def _clear_construct_import_state() -> None:
    """Drop registered constructs and cached modules so imports re-run cleanly.

    Mirrors the helper in ``test_construct_registry_postgres.py``: without it these
    tests would assert against whatever a previous test already imported, and the
    lazy export maps would never be exercised.
    """
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


@pytest.fixture
def narrow_import_env(pg_bootstrapped_engine):
    """Point construct imports at the scratch CDM, with no manifest bootstrapped.

    Clears on entry only, never on exit — deliberately. Clearing on teardown pops
    ``omop_constructs.*`` out of ``sys.modules`` and leaves a different baseline than
    the rest of the suite expects, which made
    ``test_registry_schema_artifact_postgres`` fail when it ran after
    ``test_primary_diagnosis_condition_mv`` (that test injects synthetic construct
    modules and relies on the ambient import state). Entry-only clearing matches
    ``test_construct_registry_postgres.py``.
    """
    del pg_bootstrapped_engine  # fixture sets the env; we only need its side effects
    _clear_construct_import_state()
    yield


def _import_from(module_name: str, attr: str):
    module = __import__(module_name, fromlist=[attr])
    return getattr(module, attr)


@pytest.mark.postgres
def test_consumed_exports_resolve_without_the_manifest(narrow_import_env) -> None:
    """Each name imports from its subpackage without bootstrapping the registry."""
    missing: list[str] = []
    for module_name, names in CONSUMED_EXPORTS.items():
        for name in names:
            try:
                _import_from(module_name, name)
            except (ImportError, AttributeError) as exc:
                missing.append(f"{module_name}.{name} ({type(exc).__name__}: {exc})")
    assert not missing, "downstream-consumed exports no longer resolve:\n" + "\n".join(missing)


@pytest.mark.postgres
def test_consumed_exports_are_mapped_and_declare_their_own_module(narrow_import_env) -> None:
    """A construct must be a mapped class, and must come from its own module.

    The second half is what catches a lost incidental import: if a name were only
    reachable because another module imported it, the lazy export map would be
    pointing at the wrong module and this would surface it.
    """
    for module_name, names in CONSUMED_EXPORTS.items():
        for name in names:
            obj = _import_from(module_name, name)
            assert hasattr(obj, "__table__"), f"{name} is not a mapped construct"
            assert obj.__module__.startswith(module_name), (
                f"{name} resolved to {obj.__module__}, outside {module_name}"
            )


@pytest.mark.postgres
def test_consumed_columns_are_present(narrow_import_env) -> None:
    for (module_name, name), columns in CONSUMED_COLUMNS.items():
        obj = _import_from(module_name, name)
        present = set(obj.__table__.columns.keys())
        absent = [c for c in columns if c not in present]
        assert not absent, f"{name} no longer exposes {absent}"


@pytest.mark.postgres
def test_consumed_constructs_can_be_subclassed_and_mappers_configure(narrow_import_env) -> None:
    """Downstream packages subclass these as mapped classes.

    ``configure_mappers`` is where an unregistered or unresolvable mapper fails, so
    a construct that imports fine can still break a consumer here.
    """
    bases = [
        _import_from(m, n)
        for m, names in CONSUMED_EXPORTS.items()
        for n in names
    ]
    for base in bases:
        type(f"_Probe{base.__name__}", (base,), {})
    so.configure_mappers()
