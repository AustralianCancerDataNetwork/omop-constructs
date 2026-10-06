"""OC-1 compatibility and counterexample coverage for event attachment."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from omop_alchemy.cdm.model import Measurement, Observation, Procedure_Occurrence
from omop_alchemy.cdm.model.clinical.event_metadata import clinical_event_model_spec
from omop_alchemy.toolkit.episodes.derivation import (
    EpisodeAttachmentPolicy,
    TemporalSelectionPolicy,
    TemporalSidePreference,
)
from omop_semantics.runtime.default_valuesets import runtime
from omop_constructs.alchemy.events.event_factories import (
    EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    EVENT_CONSTRUCT_ATTACHMENT_RANKING,
    attach_to_condition_episode,
    attach_to_condition_episode_by_time_window,
    attach_to_condition_episode_via_episode_event,
    measurement_attached_to_condition_episode,
    observation_attached_to_condition_episode,
    procedure_attached_to_condition_episode,
    procedure_event_core,
)


MEASUREMENT = clinical_event_model_spec(Measurement)
OBSERVATION = clinical_event_model_spec(Observation)
PROCEDURE = clinical_event_model_spec(Procedure_Occurrence)

EPISODE_OF_CARE = int(runtime.types.disease_episode_types.episode_of_care)
PROGRESSION = int(runtime.types.disease_episode_types.disease_progression)
METASTATIC = int(runtime.types.disease_episode_types.metastatic)

EpisodeRow = tuple[int, int, date, date, int]

_OVERLAPPING_EPISODES_OF_CARE: tuple[EpisodeRow, ...] = (
    (1001, 101, date(2026, 1, 15), date(2026, 2, 10), EPISODE_OF_CARE),
    (1002, 101, date(2026, 1, 18), date(2026, 2, 12), EPISODE_OF_CARE),
    (2001, 202, date(2026, 1, 10), date(2026, 2, 1), EPISODE_OF_CARE),
)


def _literal_cte(name: str, rows: Sequence[dict[str, object]]) -> sa.CTE:
    """A CTE of literal rows; SQL expressions such as typed NULLs pass through."""
    return sa.union_all(
        *(
            sa.select(
                *(
                    (value if isinstance(value, sa.ColumnElement) else sa.literal(value)).label(column)
                    for column, value in row.items()
                )
            )
            for row in rows
        )
    ).cte(name)


def _events(*rows: tuple[str, int, int, date, int]) -> sa.CTE:
    return _literal_cte(
        "events",
        [
            {
                "person_id": person_id,
                "event_id": event_id,
                "event_date": event_date,
                "event_datetime": sa.cast(sa.null(), sa.DateTime()),
                "event_concept_id": 900_001,
                "event_field_concept_id": field_concept_id,
                "event_source_table": source_table,
                "event_label": f"{source_table}-{event_id}",
            }
            for source_table, field_concept_id, person_id, event_date, event_id in rows
        ],
    )


def _episodes(rows: Sequence[EpisodeRow] = _OVERLAPPING_EPISODES_OF_CARE) -> sa.CTE:
    return _literal_cte(
        "episodes",
        [
            {
                "episode_id": episode_id,
                "person_id": person_id,
                "episode_concept_id": concept_id,
                "episode_label": f"episode-{episode_id}",
                "episode_start_date": start,
                "episode_end_date": end,
            }
            for episode_id, person_id, start, end, concept_id in rows
        ],
    )


def _links(*rows: tuple[int, int, int]) -> sa.CTE:
    return _literal_cte(
        "episode_events",
        [
            {
                "episode_id": episode_id,
                "event_id": event_id,
                "episode_event_field_concept_id": field_concept_id,
            }
            for episode_id, event_id, field_concept_id in rows
        ],
    )


def _attached(
    events: sa.CTE,
    links: sa.CTE,
    *,
    policy: EpisodeAttachmentPolicy = EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    episodes: sa.CTE | None = None,
) -> sa.Subquery:
    return attach_to_condition_episode(
        events,
        event_id_col=events.c.event_id,
        date_col=events.c.event_date,
        person_col=events.c.person_id,
        name="attached_events",
        policy=policy,
        episodes=episodes if episodes is not None else _episodes(),
        episode_events=links,
    )


def _rows(statement: sa.Subquery) -> Sequence[sa.RowMapping]:
    engine = sa.create_engine("sqlite://")
    with engine.connect() as connection:
        return connection.execute(sa.select(statement)).mappings().all()


def test_explicit_links_use_table_discriminator_person_and_precedence():
    events = _events(
        (
            MEASUREMENT.event_source_table,
            MEASUREMENT.event_field_concept_id,
            101,
            date(2026, 1, 20),
            7,
        ),
        (
            PROCEDURE.event_source_table,
            PROCEDURE.event_field_concept_id,
            101,
            date(2026, 1, 20),
            7,
        ),
        (
            OBSERVATION.event_source_table,
            OBSERVATION.event_field_concept_id,
            202,
            date(2026, 1, 20),
            7,
        ),
    )
    links = _links(
        (1001, 7, MEASUREMENT.event_field_concept_id),
        (1001, 7, MEASUREMENT.event_field_concept_id),
        (1002, 7, PROCEDURE.event_field_concept_id),
        # Correct discriminator but wrong person: this is not authoritative.
        (1001, 7, OBSERVATION.event_field_concept_id),
    )

    rows = _rows(_attached(events, links))
    identities = [
        (row["event_label"].rsplit("-", 1)[0], row["event_id"], row["episode_id"])
        for row in rows
    ]

    assert identities.count(("measurement", 7, 1001)) == 1
    assert identities.count(("procedure_occurrence", 7, 1002)) == 1
    assert ("measurement", 7, 1002) not in identities
    assert ("procedure_occurrence", 7, 1001) not in identities
    assert ("observation", 7, 2001) in identities
    assert len(identities) == len(set(identities))


def test_unlinked_fallback_selects_the_nearest_started_episode():
    events = _events(
        (
            PROCEDURE.event_source_table,
            PROCEDURE.event_field_concept_id,
            101,
            date(2026, 1, 20),
            8,
        )
    )
    rows = _rows(
        _attached(
            events,
            _links((2001, 99, PROCEDURE.event_field_concept_id)),
        )
    )

    assert [(row["event_id"], row["episode_id"]) for row in rows] == [(8, 1002)]


def test_default_attachment_contract_is_deterministic():
    assert EVENT_CONSTRUCT_ATTACHMENT_POLICY is EpisodeAttachmentPolicy.explicit_first_ranked
    assert EVENT_CONSTRUCT_ATTACHMENT_RANKING.policy is TemporalSelectionPolicy.nearest
    assert EVENT_CONSTRUCT_ATTACHMENT_RANKING.stable_id_column == "episode_id"
    assert (
        EVENT_CONSTRUCT_ATTACHMENT_RANKING.side_preference
        is TemporalSidePreference.on_or_before_anchor
    )


def test_multiple_valid_explicit_links_are_preserved():
    events = _events(
        (
            PROCEDURE.event_source_table,
            PROCEDURE.event_field_concept_id,
            101,
            date(2026, 1, 20),
            7,
        )
    )
    rows = _rows(
        _attached(
            events,
            _links(
                (1001, 7, PROCEDURE.event_field_concept_id),
                (1002, 7, PROCEDURE.event_field_concept_id),
            ),
        )
    )

    assert {(row["event_id"], row["episode_id"]) for row in rows} == {
        (7, 1001),
        (7, 1002),
    }


def test_legacy_boolean_adapter_warns_and_preserves_explicit_only_shape():
    events = _events(
        (
            PROCEDURE.event_source_table,
            PROCEDURE.event_field_concept_id,
            101,
            date(2026, 1, 20),
            7,
        )
    )
    episodes = _episodes()
    links = _links((1002, 7, PROCEDURE.event_field_concept_id))

    with pytest.warns(DeprecationWarning, match="EpisodeAttachmentPolicy"):
        legacy = attach_to_condition_episode(
            events,
            event_id_col=events.c.event_id,
            date_col=events.c.event_date,
            person_col=events.c.person_id,
            name="legacy_explicit",
            prefer_explicit_link=False,
            episodes=episodes,
            episode_events=links,
        )
    named = attach_to_condition_episode(
        events,
        event_id_col=events.c.event_id,
        date_col=events.c.event_date,
        person_col=events.c.person_id,
        name="named_explicit",
        policy=EpisodeAttachmentPolicy.explicit_only,
        episodes=episodes,
        episode_events=links,
    )

    assert tuple(legacy.c.keys()) == tuple(named.c.keys())
    assert _rows(legacy)[0]["episode_id"] == _rows(named)[0]["episode_id"] == 1002


def test_legacy_true_adapter_preserves_all_in_window_shape():
    events = _events(
        (
            PROCEDURE.event_source_table,
            PROCEDURE.event_field_concept_id,
            101,
            date(2026, 1, 20),
            8,
        )
    )

    with pytest.warns(DeprecationWarning, match="EpisodeAttachmentPolicy"):
        attached = attach_to_condition_episode(
            events,
            event_id_col=events.c.event_id,
            date_col=events.c.event_date,
            person_col=events.c.person_id,
            name="legacy_all_in_window",
            prefer_explicit_link=True,
            episodes=_episodes(),
            episode_events=_links(
                (2001, 99, PROCEDURE.event_field_concept_id),
            ),
        )

    assert {(row["event_id"], row["episode_id"]) for row in _rows(attached)} == {
        (8, 1001),
        (8, 1002),
    }


@pytest.mark.parametrize(
    ("factory", "spec"),
    (
        (measurement_attached_to_condition_episode, MEASUREMENT),
        (observation_attached_to_condition_episode, OBSERVATION),
        (procedure_attached_to_condition_episode, PROCEDURE),
    ),
)
def test_each_factory_uses_its_canonical_event_identity(factory, spec):
    attached = factory(
        name=f"{spec.event_source_table}_attachments",
        policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    )
    sql = str(
        sa.select(attached).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert f"{spec.event_field_concept_id} AS event_field_concept_id" in sql
    assert f"'{spec.event_source_table}' AS event_source_table" in sql
    assert "row_number() OVER" in sql
    assert "episode_start_date <=" in sql
    assert "abs(" in sql


def test_legacy_factory_columns_remain_stable():
    assert tuple(procedure_event_core().c.keys()) == (
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
    )
    attached = procedure_attached_to_condition_episode(
        name="procedure_attachments",
        policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    )

    # attachment_method is provenance for episode_relevant_window, which needs
    # to exempt explicitly linked rows from the proximity bound. Every caller
    # pipes this through that window, which drops the column, so no deployed
    # view shape changes; tests/test_dx_event_window_scope.py pins those.
    assert tuple(attached.c.keys()) == (
        "person_id",
        "event_id",
        "event_date",
        "event_concept_id",
        "event_label",
        "episode_id",
        "episode_concept_id",
        "episode_label",
        "episode_start_date",
        "episode_end_date",
        "episode_delta_days",
        "attachment_method",
    )


def test_deprecated_attachment_helpers_warn_and_compile():
    events = _events(
        (
            MEASUREMENT.event_source_table,
            MEASUREMENT.event_field_concept_id,
            101,
            date(2026, 1, 20),
            7,
        )
    )
    episodes = _episodes()

    with pytest.warns(DeprecationWarning, match="is deprecated"):
        explicit = attach_to_condition_episode_via_episode_event(
            events,
            event_id_col=events.c.event_id,
            date_col=events.c.event_date,
            name="deprecated_explicit_helper",
            episodes=episodes,
            episode_events=_links(
                (1001, 7, MEASUREMENT.event_field_concept_id),
            ),
        )
    with pytest.warns(DeprecationWarning, match="is deprecated"):
        fallback = attach_to_condition_episode_by_time_window(
            events,
            date_col=events.c.event_date,
            person_col=events.c.person_id,
            name="deprecated_window_helper",
            episodes=episodes,
        )

    assert _rows(explicit)[0]["episode_id"] == 1001
    assert {row["episode_id"] for row in _rows(fallback)} == {1001, 1002}


# Disease hierarchy: an episode of care and a disease child (progression or
# metastatic) nested beneath it. Cohort rows are keyed on the episode of care, so
# date-window fallback must never choose a child, while explicit links to a
# child stay valid.
ROOT = (3001, 101, date(2025, 1, 10), date(2025, 12, 31), EPISODE_OF_CARE)

_FALLBACK_ADMITS_CHILDREN = pytest.mark.xfail(
    strict=True,
    reason="Ranked fallback still admits progression and metastatic episodes as candidates.",
)


def _child(concept_id: int, *, episode_id: int = 3002) -> EpisodeRow:
    return (episode_id, 101, date(2025, 6, 1), date(2025, 12, 31), concept_id)


def _procedure(event_id: int, event_date: date) -> tuple[str, int, int, date, int]:
    return (
        PROCEDURE.event_source_table,
        PROCEDURE.event_field_concept_id,
        101,
        event_date,
        event_id,
    )


def _no_links() -> sa.CTE:
    return _links((9999, 99, PROCEDURE.event_field_concept_id))


def _pairs(statement: sa.Subquery) -> set[tuple[int, int]]:
    return {(row["event_id"], row["episode_id"]) for row in _rows(statement)}


@_FALLBACK_ADMITS_CHILDREN
@pytest.mark.parametrize("child_concept", [PROGRESSION, METASTATIC])
def test_ranked_fallback_chooses_the_episode_of_care_over_its_later_child(child_concept):
    attached = _attached(
        _events(_procedure(8, date(2025, 7, 1))),
        _no_links(),
        episodes=_episodes((ROOT, _child(child_concept))),
    )

    assert _pairs(attached) == {(8, 3001)}


@_FALLBACK_ADMITS_CHILDREN
def test_ranked_fallback_compares_episode_of_care_starts_only():
    older_root = (3001, 101, date(2024, 1, 10), date(2025, 12, 31), EPISODE_OF_CARE)
    newer_root = (3003, 101, date(2025, 3, 1), date(2025, 12, 31), EPISODE_OF_CARE)
    attached = _attached(
        _events(_procedure(8, date(2025, 7, 1))),
        _no_links(),
        episodes=_episodes((older_root, newer_root, _child(PROGRESSION))),
    )

    # The older root's child started most recently, but it does not compete,
    # and it is not remapped to its root either.
    assert _pairs(attached) == {(8, 3003)}


@_FALLBACK_ADMITS_CHILDREN
def test_event_inside_only_a_child_window_is_not_attached_by_date():
    short_root = (3001, 101, date(2025, 1, 10), date(2025, 3, 31), EPISODE_OF_CARE)
    attached = _attached(
        _events(_procedure(8, date(2025, 8, 1))),
        _no_links(),
        episodes=_episodes((short_root, _child(PROGRESSION))),
    )

    assert _pairs(attached) == set()


@pytest.mark.parametrize(
    "policy",
    [
        EpisodeAttachmentPolicy.explicit_only,
        EpisodeAttachmentPolicy.explicit_first_ranked,
        EpisodeAttachmentPolicy.explicit_first_all_in_window,
    ],
)
def test_valid_explicit_links_to_a_child_are_kept(policy):
    inside_root_window = _procedure(8, date(2025, 7, 1))
    outside_every_window = _procedure(9, date(2024, 6, 1))
    linked_to_both = _procedure(10, date(2025, 7, 1))
    attached = _attached(
        _events(inside_root_window, outside_every_window, linked_to_both),
        _links(
            (3002, 8, PROCEDURE.event_field_concept_id),
            (3002, 9, PROCEDURE.event_field_concept_id),
            (3001, 10, PROCEDURE.event_field_concept_id),
            (3002, 10, PROCEDURE.event_field_concept_id),
        ),
        policy=policy,
        episodes=_episodes((ROOT, _child(PROGRESSION))),
    )

    assert _pairs(attached) == {(8, 3002), (9, 3002), (10, 3001), (10, 3002)}


def test_all_in_window_fallback_still_includes_disease_children():
    attached = _attached(
        _events(_procedure(8, date(2025, 7, 1))),
        _no_links(),
        policy=EpisodeAttachmentPolicy.explicit_first_all_in_window,
        episodes=_episodes((ROOT, _child(PROGRESSION))),
    )

    assert _pairs(attached) == {(8, 3001), (8, 3002)}
