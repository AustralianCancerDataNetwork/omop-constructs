"""Pin the diagnosis-linked event view shapes and the window's scope.

``episode_relevant_window`` bounds *proximity-guessed* attachments. A recorded
``episode_event`` link is a clinical assertion, so its distance from the episode
start is not evidence against it; applying the guess bound to an explicit
attachment discards documented fact and, under explicit-first, leaves no
fallback to replace it. These tests pin both the exemption and the output shapes
the materialized views select positionally.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

WINDOWED_SHAPES = {
    "bsa_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "creat_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "dtherm_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "dx_all_measurements": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "value_as_concept_id",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "ecog_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "value_as_concept_id",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "egfr_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "fev1_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "height_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "pyh_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "weight_change_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "weight_dx": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_as_number",
        "unit_concept_id",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "dx_all_observations": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "value_concept_id",
        "qualifier_concept_id",
        "value_as_number",
        "qualifier_concept_id_1",
        "observation_concept_id",
        "observation_date",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ],
    "dx_all_procedures": [
        "mv_id",
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "procedure_concept_id",
        "procedure_datetime",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days"
    ]
}


@pytest.fixture(scope="module")
def windowed_selects():
    from omop_constructs.alchemy.events import (
        event_queries,
        observation_queries,
        procedure_queries,
    )

    found = {}
    for module in (event_queries, observation_queries, procedure_queries):
        for name in dir(module):
            value = getattr(module, name)
            if not name.startswith("_") and isinstance(value, sa.Subquery):
                if "mv_id" in value.c:
                    found[name] = value
    return found


@pytest.mark.parametrize(("name", "expected"), sorted(WINDOWED_SHAPES.items()))
def test_windowed_select_shape_is_unchanged(windowed_selects, name, expected):
    assert [column.key for column in windowed_selects[name].c] == expected


def test_every_windowed_select_is_covered(windowed_selects):
    assert set(windowed_selects) == set(WINDOWED_SHAPES)


def test_attachment_method_is_consumed_not_exposed(windowed_selects):
    """The window needs the provenance column; the views must not carry it."""
    for name, query in windowed_selects.items():
        assert "attachment_method" not in query.c, name


@pytest.mark.parametrize("name", sorted(WINDOWED_SHAPES))
def test_explicit_attachments_are_exempt_from_the_window(windowed_selects, name):
    """An explicitly linked event survives regardless of episode_delta_days."""
    # These selects carry a construct with only a PostgreSQL compiler, so the
    # default renderer cannot stringify them.
    compiled = " ".join(
        str(windowed_selects[name].select().compile(dialect=postgresql.dialect())).split()
    )
    where = compiled[compiled.rindex("WHERE"):]
    assert "attachment_method" in where, name


@pytest.mark.parametrize("name", sorted(WINDOWED_SHAPES))
def test_the_window_bound_respects_the_recorded_episode_end(
    windowed_selects, name
):
    """The bound must be the attachment builder's, not a fixed days-post cap.

    Comparing against ``episode_start_date + days_post`` ignores
    ``episode_end_date`` and truncates any closed episode longer than that
    horizon partway through itself.
    """
    compiled = " ".join(
        str(windowed_selects[name].select().compile(dialect=postgresql.dialect())).split()
    )
    where = compiled[compiled.rindex("WHERE"):]
    assert "coalesce" in where.lower(), name
    assert "episode_end_date" in where, name
