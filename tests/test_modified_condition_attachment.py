"""Executable OC-CM3 coverage for the ModifiedCondition attachment spine."""

from __future__ import annotations

from datetime import date

import pytest
import sqlalchemy as sa


def _create_empty_modifier_views(conn: sa.Connection) -> None:
    """Create just the nullable modifier inputs required by the real join."""
    for name in ("t_stage_mv", "n_stage_mv", "m_stage_mv", "group_stage_mv"):
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE {name} (
                    person_id INTEGER,
                    meas_event_field_concept_id INTEGER,
                    measurement_event_id INTEGER,
                    stage_id INTEGER,
                    stage_date DATE,
                    stage_concept_id INTEGER,
                    stage_label TEXT
                )
                """
            )
        )
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
    for name in ("grade_modifier_mv", "metastatic_disease_modifier_mv"):
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE {name} (
                    person_id INTEGER,
                    meas_event_field_concept_id INTEGER,
                    measurement_event_id INTEGER,
                    measurement_id INTEGER,
                    measurement_date DATE,
                    measurement_concept_id INTEGER,
                    concept_name TEXT,
                    value_as_concept_id INTEGER
                )
                """
            )
        )
    conn.execute(
        sa.text(
            """
            CREATE TABLE size_modifier_mv (
                person_id INTEGER,
                meas_event_field_concept_id INTEGER,
                measurement_event_id INTEGER,
                measurement_id INTEGER,
                measurement_date DATE,
                measurement_concept_id INTEGER,
                concept_name TEXT,
                value_as_number NUMERIC,
                unit_concept_id INTEGER
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            CREATE TABLE laterality_modifier_mv (
                person_id INTEGER,
                meas_event_field_concept_id INTEGER,
                measurement_event_id INTEGER,
                measurement_id INTEGER,
                measurement_date DATE,
                measurement_concept_id INTEGER,
                concept_name TEXT,
                value_as_concept_id INTEGER
            )
            """
        )
    )


@pytest.mark.postgres
def test_modified_condition_keeps_only_validated_explicit_attachment(
    pg_bootstrapped_engine,
):
    """A cross-person Episode_Event link leaves the condition on its null spine.

    The first link has matching condition identity, Field concept, episode, and
    person, so it is retained even though its episode concept is outside the
    condition-episode MV's configured subset.  The second has the same Field
    concept but an episode owned by another person, so Alchemy rejects it
    instead of assigning that episode to the condition.
    """
    from omop_alchemy.cdm.model import Condition_Occurrence, Episode, Episode_Event
    from omop_constructs.alchemy.modifiers.condition_modifier_join import (
        all_stage_join,
        condition_attachment_diagnostics,
        modified_conditions_join,
    )
    from omop_semantics.runtime.default_valuesets import runtime

    engine = pg_bootstrapped_engine
    condition_field = runtime.modifiers.modifier_fields.condition_occurrence_id
    with engine.begin() as conn:
        _create_empty_modifier_views(conn)
        for table in ("concept", "condition_occurrence", "episode", "episode_event"):
            conn.execute(sa.text(f"ALTER TABLE {table} DISABLE TRIGGER ALL"))
        conn.execute(
            sa.text(
                """
                INSERT INTO concept VALUES
                    (900001, 'Synthetic condition A', 'Condition', 'SNOMED',
                     'Clinical Finding', 'S', 'SYN-A',
                     DATE '1970-01-01', DATE '2099-12-31', NULL),
                    (900002, 'Synthetic condition B', 'Condition', 'SNOMED',
                     'Clinical Finding', 'S', 'SYN-B',
                     DATE '1970-01-01', DATE '2099-12-31', NULL),
                    (900003, 'Synthetic other episode', 'Episode', 'SNOMED',
                     'Clinical Finding', 'S', 'SYN-OTHER',
                     DATE '1970-01-01', DATE '2099-12-31', NULL)
                """
            )
        )
        conn.execute(
            Condition_Occurrence.__table__.insert(),
            [
                {
                    "condition_occurrence_id": 1,
                    "person_id": 101,
                    "condition_concept_id": 900001,
                    "condition_type_concept_id": 0,
                    "condition_start_date": date(2020, 1, 2),
                },
                {
                    "condition_occurrence_id": 2,
                    "person_id": 202,
                    "condition_concept_id": 900002,
                    "condition_type_concept_id": 0,
                    "condition_start_date": date(2020, 1, 3),
                },
            ],
        )
        conn.execute(
            sa.text(
                """
                INSERT INTO all_stage_modifier_mv VALUES
                    (999, 801, DATE '2020-01-04', 'wrong person', 1, :field, 910001),
                    (202, 802, DATE '2020-01-05', 'matching person', 2, :field, 910002)
                """
            ),
            {"field": condition_field},
        )
        conn.execute(
            sa.text(
                """
                INSERT INTO t_stage_mv VALUES
                    (999, :field, 1, 701, DATE '2020-01-04', 910001, 'wrong person'),
                    (101, :field, 1, 702, DATE '2020-01-05', 910002, 'matching person')
                """
            ),
            {"field": condition_field},
        )
        conn.execute(
            Episode.__table__.insert(),
            {
                "episode_id": 501,
                "person_id": 101,
                "episode_start_date": date(2020, 1, 1),
                "episode_concept_id": 900003,
                "episode_object_concept_id": 900003,
                "episode_type_concept_id": 900003,
            },
        )
        conn.execute(
            Episode_Event.__table__.insert(),
            [
                {
                    "episode_id": 501,
                    "event_id": 1,
                    "episode_event_field_concept_id": condition_field,
                },
                {
                    "episode_id": 501,
                    "event_id": 2,
                    "episode_event_field_concept_id": condition_field,
                },
            ],
        )
        for table in ("episode_event", "episode", "condition_occurrence", "concept"):
            conn.execute(sa.text(f"ALTER TABLE {table} ENABLE TRIGGER ALL"))

        modified_conditions = modified_conditions_join.subquery()
        rows = conn.execute(
            sa.select(
                modified_conditions.c.condition_occurrence_id,
                modified_conditions.c.condition_episode,
                modified_conditions.c.t_stage_id,
            ).order_by(modified_conditions.c.condition_occurrence_id)
        ).all()
        all_stage = all_stage_join.subquery()
        stage_rows = conn.execute(
            sa.select(
                all_stage.c.condition_occurrence_id,
                all_stage.c.condition_episode,
                all_stage.c.stage_id,
            ).order_by(all_stage.c.condition_occurrence_id)
        ).all()
        diagnostics = conn.execute(condition_attachment_diagnostics).mappings().all()

    assert rows == [(1, 501, 702), (2, None, None)]
    assert stage_rows == [(1, 501, None), (2, None, 802)]
    assert any(
        row["diagnostic_code"] == "person_mismatch"
        and row["event_id"] == 2
        and row["episode_id"] == 501
        for row in diagnostics
    )
