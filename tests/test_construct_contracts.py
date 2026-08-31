"""Validate the D3 contract manifest against the shipped construct registry.

These are the OC-0 gate tests. They fail when the manifest and the registry
disagree — a construct added without a declared grain, a construct removed
without its contract, or a declared key naming a column that no longer exists.
That last one is the point: OC-B3/B4/H1 cannot be worked on safely until every
affected view has a key that is at least *executable* as a uniqueness
assertion.

What these tests deliberately do NOT check is whether a declared key actually
holds against data. Declared keys are intended keys, and most of them are
violated today by design — see each contract's ``known_violations``. Measuring
the real duplicate counts needs a populated CDM and belongs to
``scripts/release_validation/collect_key_metrics.py``.

Marked ``postgres`` because importing the construct manifest resolves the
semantics registry against a live database at module scope (OC-H2). The
vocabulary tables must exist; they do not need rows.
"""

from __future__ import annotations

import importlib
import sys

import pytest
from orm_loader.helpers import Base

from omop_constructs import get_complete_construct_registry
from omop_constructs.core.contracts import (
    CONTRACTS_FILENAME,
    find_contracts_path,
    get_contracts,
    load_contracts,
)
from omop_constructs.core.errors import ConstructSpecError

# Mirrors oa_cohorts/measurables/*.py. Any construct a downstream package
# imports by name has to be part of the 1.0 public surface — a construct cannot
# be simultaneously excluded from the contract and consumed by a consumer.
OA_COHORTS_CONSUMED_CLASSES = frozenset(
    {
        "ConditionTreatmentEpisode",
        "ConditionTreatmentIntentMV",
        "ConsultWindowMV",
        "DxMeasurementMV",
        "DxObservationMV",
        "DxProcedureMV",
        "DxRelevantVisitMV",
        "DxTreatStartMV",
        "ModifiedCondition",
        "PersonDemography",
        "PrimaryDiagnosisConditionMV",
        "StageModifier",
        "SurgicalProcedureMV",
        "TreatmentEnvelopeMV",
    }
)


def _clear_construct_import_state() -> None:
    """Drop registered constructs and cached modules so imports re-run cleanly.

    Same helper as ``test_construct_registry_postgres.py``; see the note there.
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
            module_name.startswith(
                ("omop_constructs.alchemy", "omop_constructs.semantics")
            )
            or module_name in {"omop_constructs.bootstrap", "omop_constructs.bootstrap_modules"}
        ):
            sys.modules.pop(module_name, None)


@pytest.fixture(scope="module")
def contracts():
    return get_contracts()


#: Built once for this module. Every test below only reads registry metadata —
#: names, classes, columns, dependencies — and the registry is a process-global,
#: so rebuilding it per test would re-import every construct module against the
#: live declarative base and multiply the OC-M4 warnings by the test count for
#: no added coverage.
_REGISTRY_CACHE: list = []


@pytest.fixture
def registry(pg_bootstrapped_engine):
    del pg_bootstrapped_engine  # fixture only needs to set the import environment
    if not _REGISTRY_CACHE:
        _clear_construct_import_state()
        _REGISTRY_CACHE.append(get_complete_construct_registry())
    return _REGISTRY_CACHE[0]


# ---------------------------------------------------------------------------
# Manifest structure — no database needed
# ---------------------------------------------------------------------------

def test_manifest_parses_and_declares_its_schema_version(contracts):
    assert contracts.meta["schema_version"] == "1"
    assert contracts.meta["milestone"] == "OC-0"
    assert len(contracts) > 0


def test_find_contracts_path_supports_an_installed_package_layout(tmp_path):
    """The installed CLI finds the manifest beside the package, without a checkout."""
    package = tmp_path / "site-packages" / "omop_constructs"
    module = package / "core" / "contracts.py"
    module.parent.mkdir(parents=True)
    manifest = package / CONTRACTS_FILENAME
    manifest.write_text("[meta]\nschema_version = '1'\n", encoding="utf-8")

    assert find_contracts_path(module) == manifest


def test_non_key_findings_do_not_mark_a_reviewed_key_unsatisfied(contracts):
    """Findings cover more than uniqueness; key status has its own declaration."""
    assert contracts.get("t_stage_mv").known_violations
    assert contracts.get("t_stage_mv").satisfies_declared_key
    assert not contracts.get("dx_observation_mv").satisfies_declared_key


def test_every_construct_scoped_finding_is_cited_by_a_contract(contracts):
    """A construct-scoped finding with no citation is a stale entry.

    Package-scoped findings — schema lifecycle, import-time I/O, mapper
    registration — have no single construct to attach to and are exempt.
    """
    uncited = [
        finding.finding_id
        for finding in contracts.construct_scoped_findings()
        if not contracts.with_violation(finding.finding_id)
    ]
    assert not uncited, f"construct-scoped findings cited by no contract: {uncited}"


def test_nullable_key_columns_drive_the_nulls_not_distinct_requirement(contracts):
    """A key with a nullable column cannot use a plain PostgreSQL unique index.

    PostgreSQL treats NULLs as distinct by default, so a null-spine row would
    never collide with itself. Any construct that OC-3 will build a unique index
    for must therefore say so, because OC-3 reads the index columns from here.

    Restricted to concurrent-refresh-eligible constructs: an ineligible one has
    no index to build yet, and its note already explains what blocks it.
    """
    missing_note = [
        contract.name
        for contract in contracts
        if contract.concurrent_refresh_eligible
        and contract.requires_nulls_not_distinct
        and "NULLS NOT DISTINCT" not in (contract.concurrent_refresh_note or "")
    ]
    assert not missing_note, (
        "contracts with nullable key columns that do not explain the "
        f"NULLS NOT DISTINCT requirement: {missing_note}"
    )


def test_concurrent_refresh_ineligibility_is_explained(contracts):
    unexplained = [
        contract.name
        for contract in contracts
        if not contract.concurrent_refresh_eligible
        and not contract.concurrent_refresh_note
    ]
    assert not unexplained, (
        f"contracts not eligible for concurrent refresh with no reason given: {unexplained}"
    )


def test_exclusion_from_the_public_surface_is_explained(contracts):
    unexplained = [
        contract.name
        for contract in contracts.excluded_from_1_0()
        if not contract.public_api_note
    ]
    assert not unexplained, (
        f"contracts excluded from the 1.0 surface with no reason given: {unexplained}"
    )


def test_a_construct_with_consumer_rule_targets_is_in_the_public_surface(contracts):
    """A live oa-cohorts measurable cannot rest on a non-public construct."""
    contradictions = [
        contract.name
        for contract in contracts
        if contract.oa_cohorts_rule_targets and not contract.public_api_1_0
    ]
    assert not contradictions, (
        f"contracts bound to oa-cohorts rule targets but excluded from 1.0: {contradictions}"
    )


def test_loader_rejects_a_nullable_column_outside_the_logical_key(tmp_path):
    path = tmp_path / "construct-contracts.toml"
    path.write_text(
        """
[meta]
schema_version = "1"

[constructs.example_mv]
class_name = "ExampleMV"
module = "example"
family = "events"
grain = "one row per thing"
logical_key = ["thing_id"]
logical_key_complete = true
key_nullable_columns = ["not_in_the_key"]
unique_index_columns = ["thing_id"]
orm_primary_key = ["thing_id"]
surrogate_kind = "source_identifier"
surrogate_stable_across_refresh = true
concurrent_refresh_eligible = true
public_api_1_0 = true
lung_report_role = "unused"
inputs = [{ name = "thing", kind = "cdm_table", fan_out = "one_to_one" }]
""",
        encoding="utf-8",
    )

    with pytest.raises(ConstructSpecError, match="not part of logical_key"):
        load_contracts(path)


def test_loader_rejects_an_unknown_fan_out(tmp_path):
    path = tmp_path / "construct-contracts.toml"
    path.write_text(
        """
[meta]
schema_version = "1"

[constructs.example_mv]
class_name = "ExampleMV"
module = "example"
family = "events"
grain = "one row per thing"
logical_key = ["thing_id"]
logical_key_complete = true
key_nullable_columns = []
unique_index_columns = ["thing_id"]
orm_primary_key = ["thing_id"]
surrogate_kind = "source_identifier"
surrogate_stable_across_refresh = true
concurrent_refresh_eligible = true
public_api_1_0 = true
lung_report_role = "unused"
inputs = [{ name = "thing", kind = "cdm_table", fan_out = "sometimes" }]
""",
        encoding="utf-8",
    )

    with pytest.raises(ConstructSpecError, match="unknown fan_out 'sometimes'"):
        load_contracts(path)


def test_loader_rejects_a_violation_citing_an_undefined_finding(tmp_path):
    path = tmp_path / "construct-contracts.toml"
    path.write_text(
        """
[meta]
schema_version = "1"

[constructs.example_mv]
class_name = "ExampleMV"
module = "example"
family = "events"
grain = "one row per thing"
logical_key = ["thing_id"]
logical_key_complete = true
key_nullable_columns = []
unique_index_columns = ["thing_id"]
orm_primary_key = ["thing_id"]
surrogate_kind = "source_identifier"
surrogate_stable_across_refresh = true
concurrent_refresh_eligible = true
public_api_1_0 = true
lung_report_role = "unused"
known_violations = ["OC-NOPE"]
inputs = [{ name = "thing", kind = "cdm_table", fan_out = "one_to_one" }]
""",
        encoding="utf-8",
    )

    with pytest.raises(ConstructSpecError, match="undefined findings"):
        load_contracts(path)


# ---------------------------------------------------------------------------
# Registry coverage and key-column validation
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_every_registered_construct_has_a_contract(registry, contracts):
    """A construct with no declared grain must not reach a release candidate."""
    registered = {item.name for item in registry.plan()}
    undeclared = sorted(registered - set(contracts.contracts))
    assert not undeclared, (
        "registered constructs with no entry in construct-contracts.toml: "
        f"{undeclared}"
    )


@pytest.mark.postgres
def test_every_contract_names_a_registered_construct(registry, contracts):
    """Catches a contract left behind after a construct was removed or renamed."""
    registered = {item.name for item in registry.plan()}
    orphaned = sorted(set(contracts.contracts) - registered)
    assert not orphaned, (
        f"contracts for constructs that are not registered: {orphaned}"
    )


@pytest.mark.postgres
def test_contract_class_and_module_match_the_registered_construct(registry, contracts):
    mismatches: list[str] = []
    for item in registry.plan():
        cls = registry.get(item.name)
        contract = contracts.get(item.name)
        if contract.class_name != cls.__name__:
            mismatches.append(
                f"{item.name}: contract says class {contract.class_name!r}, "
                f"registry has {cls.__name__!r}"
            )
        if contract.module != cls.__module__:
            mismatches.append(
                f"{item.name}: contract says module {contract.module!r}, "
                f"registry has {cls.__module__!r}"
            )
    assert not mismatches, "\n".join(mismatches)


@pytest.mark.postgres
@pytest.mark.parametrize(
    "field_name",
    ["logical_key", "unique_index_columns", "orm_primary_key"],
)
def test_declared_key_columns_exist_on_the_mapper_and_the_selectable(
    registry,
    contracts,
    field_name,
):
    """Every declared column must exist in both the mapper and the MV select.

    Checking both sides matters: a column can survive on the mapper after the
    query stopped producing it, and ``compile_check`` only requires mapped
    columns to be a subset of the selectable, so it would not catch a contract
    naming a selectable-only column either.
    """
    problems: list[str] = []
    for item in registry.plan():
        cls = registry.get(item.name)
        contract = contracts.get(item.name)

        mapped = set(cls.__table__.columns.keys())
        selectable = set(cls.__mv_select__.subquery().c.keys())

        for column in getattr(contract, field_name):
            if column not in mapped:
                problems.append(
                    f"{item.name}.{field_name}: '{column}' is not a mapped column "
                    f"(mapped: {sorted(mapped)})"
                )
            elif column not in selectable:
                problems.append(
                    f"{item.name}.{field_name}: '{column}' is mapped but absent from "
                    f"the materialized-view select"
                )
    assert not problems, "\n".join(problems)


@pytest.mark.postgres
def test_declared_orm_primary_key_matches_the_mapper(registry, contracts):
    """The contract records the real ORM key so OC-B3 contradictions stay visible.

    The manifest is where a reader compares the ORM identity against the grain,
    so a drifted record would hide exactly the problem it exists to expose.
    """
    mismatches: list[str] = []
    for item in registry.plan():
        cls = registry.get(item.name)
        contract = contracts.get(item.name)
        actual = tuple(col.name for col in cls.__table__.primary_key.columns)
        if contract.orm_primary_key != actual:
            mismatches.append(
                f"{item.name}: contract records ORM primary key "
                f"{list(contract.orm_primary_key)}, mapper has {list(actual)}"
            )
    assert not mismatches, "\n".join(mismatches)


@pytest.mark.postgres
def test_contract_input_dependencies_are_declared_registry_dependencies(
    registry,
    contracts,
):
    """A construct input naming another construct must appear in ``__deps__``.

    Build order comes from ``__deps__``, so a construct that reads another one
    without declaring it can be built against a stale or missing view.
    """
    problems: list[str] = []
    known = {item.name for item in registry.plan()}
    for item in registry.plan():
        contract = contracts.get(item.name)
        declared = set(registry.get(item.name).__deps__)
        for input_ in contract.inputs:
            if input_.kind != "construct":
                continue
            # Input names carry a parenthetical role for constructs read more
            # than once in a query, e.g. "dx_visit_mv (specialist visit)".
            referenced = input_.name.split(" (")[0]
            if referenced not in known:
                problems.append(
                    f"{item.name}: input '{input_.name}' names an unregistered "
                    f"construct '{referenced}'"
                )
            elif referenced not in declared:
                problems.append(
                    f"{item.name}: reads construct '{referenced}' but does not "
                    f"declare it in __deps__ {sorted(declared)}"
                )
    assert not problems, "\n".join(problems)


@pytest.mark.postgres
def test_downstream_consumed_constructs_are_in_the_public_surface(registry, contracts):
    """Every class oa-cohorts imports must be public in the 1.0 contract."""
    by_class = {contracts.get(item.name).class_name: item.name for item in registry.plan()}

    unresolved = sorted(OA_COHORTS_CONSUMED_CLASSES - set(by_class))
    assert not unresolved, (
        f"oa-cohorts consumes classes that are no longer registered: {unresolved}"
    )

    excluded = sorted(
        class_name
        for class_name in OA_COHORTS_CONSUMED_CLASSES
        if not contracts.get(by_class[class_name]).public_api_1_0
    )
    assert not excluded, (
        f"oa-cohorts consumes constructs excluded from the 1.0 surface: {excluded}"
    )
