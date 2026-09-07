"""Pin the compatibility surface of the modifier wrappers (OC-CM1).

The four wrappers named in the delivery plan are kept for one pre-1.0 cycle and
reimplemented over the omop-alchemy modifier toolkit. The rule that makes that
safe is narrow: *wrapper output labels preserve the current materialized-view
selectables*. Every modifier materialized view is built with
``select_all_columns(<wrapper output>)``, so a changed column set or order
silently changes the deployed view's shape, and the release harness reports a
changed column set as non-comparable rather than as a data difference.

These are characterisation tests. The expected lists were captured from the
pre-migration implementation, so a diff here means the deployed view shape moved
and needs a deliberate decision, not a quiet edit.

They also assert the module imports without touching a database (CM-H2) and that
ranked selection partitions on the whole target identity (CM-B1).
"""

from __future__ import annotations

import pytest

# Captured from the pre-migration wrappers. Order matters: the materialized
# views select these positionally.
STAGE_COLUMNS = [
    "person_id",
    "stage_id",
    "stage_date",
    "stage_datetime",
    "stage_concept_id",
    "measurement_event_id",
    "meas_event_field_concept_id",
    "stage_label",
    "stage_type",
    "rn",
]

MODIFIER_BASE_COLUMNS = [
    "person_id",
    "measurement_event_id",
    "meas_event_field_concept_id",
    "measurement_concept_id",
    "measurement_id",
    "measurement_date",
    "concept_name",
]

EXPECTED_SHAPES = {
    "t_stage_select": STAGE_COLUMNS,
    "n_stage_select": STAGE_COLUMNS,
    "m_stage_select": STAGE_COLUMNS,
    "group_stage_select": STAGE_COLUMNS,
    "laterality_select": [*MODIFIER_BASE_COLUMNS, "value_as_concept_id", "rn"],
    "size_select": [*MODIFIER_BASE_COLUMNS, "value_as_number", "unit_concept_id", "rn"],
    "grade_select": [*MODIFIER_BASE_COLUMNS, "rn"],
    "mets_select": [*MODIFIER_BASE_COLUMNS, "value_as_concept_id", "rn"],
    # Deliberately unranked: the long-form stage stream carries no rn.
    "all_stage_select": MODIFIER_BASE_COLUMNS,
}

# Captured before the OC-CM3 null-spine correction.  These two materialized
# views are defined with select_all_columns(), so preserving names and order is
# part of preserving their deployed public contract.
STAGE_MODIFIER_COLUMNS = [
    "mv_id",
    "person_id",
    "condition_start_date",
    "condition_occurrence_id",
    "condition_source_value",
    "condition_concept_id",
    "condition_concept",
    "condition_code",
    "condition_episode",
    "stage_id",
    "stage_date",
    "stage_concept_id",
    "stage_label",
]

MODIFIED_CONDITION_COLUMNS = [
    "mv_id",
    "person_id",
    "condition_start_date",
    "condition_occurrence_id",
    "condition_source_value",
    "condition_concept_id",
    "condition_concept",
    "condition_code",
    "condition_episode",
    "t_stage_id",
    "t_stage_date",
    "t_stage_concept_id",
    "t_stage_label",
    "n_stage_id",
    "n_stage_date",
    "n_stage_concept_id",
    "n_stage_label",
    "m_stage_id",
    "m_stage_date",
    "m_stage_concept_id",
    "m_stage_label",
    "group_stage_id",
    "group_stage_date",
    "group_stage_concept_id",
    "group_stage_label",
    "grade_id",
    "grade_date",
    "grade_concept_id",
    "grade_label",
    "size_id",
    "size_date",
    "size_concept_id",
    "size_value",
    "size_unit_concept_id",
    "size_label",
    "laterality_id",
    "laterality_date",
    "laterality_concept_id",
    "laterality_label",
    "metastatic_disease_id",
    "metastatic_disease_date",
    "metastatic_disease_concept_id",
    "metastatic_disease_label",
]


@pytest.fixture(scope="module")
def modifier_joins():
    from omop_constructs.alchemy.modifiers import modifier_joins

    return modifier_joins


@pytest.mark.parametrize(("name", "expected"), sorted(EXPECTED_SHAPES.items()))
def test_wrapper_output_shape_is_unchanged(modifier_joins, name, expected):
    """A changed column set changes the deployed materialized view."""
    query = getattr(modifier_joins, name)
    assert [column.key for column in query.c] == expected


def test_every_modifier_select_is_covered(modifier_joins):
    """A new modifier select must be added here, not silently shipped."""
    import sqlalchemy as sa

    exported = {
        name
        for name in dir(modifier_joins)
        if not name.startswith("_")
        and isinstance(getattr(modifier_joins, name), sa.Subquery)
    }
    assert exported == set(EXPECTED_SHAPES)


def test_condition_modifier_view_shapes_are_unchanged():
    """The OC-CM3 join correction must not move either deployed view shape."""
    from omop_constructs.alchemy.modifiers.condition_modifier_mv import (
        ModifiedCondition,
        StageModifier,
    )

    assert list(StageModifier.__mv_select__.exported_columns.keys()) == STAGE_MODIFIER_COLUMNS
    assert (
        list(ModifiedCondition.__mv_select__.exported_columns.keys())
        == MODIFIED_CONDITION_COLUMNS
    )


def test_ranked_wrappers_select_one_row_per_target(modifier_joins):
    """Ranked selection must partition on the full target identity.

    Partitioning on ``measurement_event_id`` alone is CM-B1: OMOP event IDs are
    unique within a table, not across tables, so a Condition Occurrence 7 and a
    Procedure Occurrence 7 would compete in one window.
    """
    for name in EXPECTED_SHAPES:
        if name == "all_stage_select":
            continue
        compiled = str(getattr(modifier_joins, name)).lower()
        assert "meas_event_field_concept_id" in compiled, name


def test_importing_modifier_joins_performs_no_database_work():
    """Importing a construct module must not require a reachable CDM.

    Measured in a subprocess on purpose. In-process the resolver registry is
    already warm from earlier imports, so the connections happen once and a
    same-process check passes while the defect is still present.
    """
    import subprocess
    import sys

    probe = """
import sqlalchemy as sa
calls = []
for attr in ("connect", "raw_connection"):
    original = getattr(sa.engine.Engine, attr)
    def spy(self, *a, _o=original, _n=attr, **k):
        calls.append(_n)
        return _o(self, *a, **k)
    setattr(sa.engine.Engine, attr, spy)
from omop_constructs.alchemy.modifiers import modifier_joins  # noqa: F401
print(len(calls))
"""
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert int(result.stdout.strip().splitlines()[-1]) == 0
