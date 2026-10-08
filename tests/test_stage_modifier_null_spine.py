"""Regression coverage for the optional-stage spine in ``StageModifier``."""

from __future__ import annotations

from datetime import date

import pytest
import sqlalchemy as sa


@pytest.mark.postgres
def test_unstaged_condition_survives_all_stage_join(pg_bootstrapped_engine):
    """Optional episode, stage, and stage-label evidence preserves the spine.

    This executes the real selectable over synthetic OMOP rows.  Before OC-CM3,
    the inner ``stage_concept`` lookup removed this row after the outer modifier
    join had correctly produced NULLs.  The second stage row uses an absent
    stage concept and must retain its stage identity with only the label NULL.
    """
    from omop_alchemy.cdm.model import Condition_Occurrence
    from omop_constructs.alchemy.modifiers.condition_modifier_join import (
        all_stage_join,
    )
    from omop_semantics.runtime.default_valuesets import runtime

    engine = pg_bootstrapped_engine
    condition_field = runtime.modifiers.modifier_fields.condition_occurrence_id
    with engine.begin() as conn:
        # The scratch CDM has OMOP source tables but not construct materialized
        # views.  An empty stand-in is sufficient: this counterexample is the
        # no-modifier branch of the outer join.
        # The disposable schema starts with empty vocabulary reference tables.
        # This query only needs the resolved condition label, so bypass their
        # circular bootstrap foreign keys rather than turning this into a
        # vocabulary fixture.
        conn.execute(sa.text("ALTER TABLE concept DISABLE TRIGGER ALL"))
        conn.execute(
            sa.text(
                """
                CREATE TABLE all_stage_modifier_mv (
                    person_id INTEGER,
                    measurement_id INTEGER PRIMARY KEY,
                    measurement_date DATE,
                    concept_name TEXT,
                    measurement_event_id INTEGER,
                    meas_event_field_concept_id INTEGER,
                    measurement_concept_id INTEGER
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                INSERT INTO concept VALUES (
                    900001, 'Synthetic condition', 'Condition', 'SNOMED',
                    'Clinical Finding', 'S', 'SYN-COND',
                    DATE '1970-01-01', DATE '2099-12-31', NULL
                )
                """
            )
        )
        conn.execute(sa.text("ALTER TABLE concept ENABLE TRIGGER ALL"))
        conn.execute(sa.text("ALTER TABLE condition_occurrence DISABLE TRIGGER ALL"))
        conn.execute(
            Condition_Occurrence.__table__.insert(),
            {
                "condition_occurrence_id": 1,
                "person_id": 101,
                "condition_concept_id": 900001,
                "condition_type_concept_id": 0,
                "condition_start_date": date(2020, 1, 2),
                "condition_source_value": "synthetic-condition",
            },
        )
        conn.execute(sa.text("ALTER TABLE condition_occurrence ENABLE TRIGGER ALL"))
        conn.execute(
            sa.text(
                """
                INSERT INTO all_stage_modifier_mv VALUES (
                    101, 701, DATE '2020-01-04', 'Unresolved stage', 1,
                    :field, 910001
                )
                """
            ),
            {"field": condition_field},
        )

        rows = conn.execute(all_stage_join).mappings().all()

    assert [
        {
            key: value
            for key, value in row.items()
            if key != "mv_id"
        }
        for row in rows
    ] == [
        {
            "person_id": 101,
            "condition_start_date": date(2020, 1, 2),
            "condition_occurrence_id": 1,
            "condition_source_value": "synthetic-condition",
            "condition_concept_id": 900001,
            "condition_concept": "Synthetic condition",
            "condition_code": "SYN-COND",
            "condition_episode": None,
            "stage_id": 701,
            "stage_date": date(2020, 1, 4),
            "stage_concept_id": 910001,
            "stage_label": None,
        }
    ]
