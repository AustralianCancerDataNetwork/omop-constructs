"""OC-1 compatibility and counterexample coverage for event attachment."""

from __future__ import annotations

from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from omop_alchemy.cdm.model import Measurement, Observation, Procedure_Occurrence
from omop_alchemy.toolkit.core.events import clinical_event_model_spec
from omop_alchemy.toolkit.episodes.derivation import EpisodeAttachmentPolicy
from omop_constructs.alchemy.events.event_factories import (
    EVENT_CONSTRUCT_ATTACHMENT_POLICY,
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


def _events(*rows: tuple[str, int, int, date, int]) -> sa.CTE:
    return sa.union_all(
        *(
            sa.select(
                sa.literal(person_id).label("person_id"),
                sa.literal(event_id).label("event_id"),
                sa.literal(event_date).label("event_date"),
                sa.cast(sa.null(), sa.DateTime()).label("event_datetime"),
                sa.literal(900_001).label("event_concept_id"),
                sa.literal(field_concept_id).label("event_field_concept_id"),
                sa.literal(source_table).label("event_source_table"),
                sa.literal(f"{source_table}-{event_id}").label("event_label"),
            )
            for source_table, field_concept_id, person_id, event_date, event_id in rows
        )
    ).cte("events")


def _episodes() -> sa.CTE:
    rows = (
        (1001, 101, date(2026, 1, 15), date(2026, 2, 10)),
        (1002, 101, date(2026, 1, 18), date(2026, 2, 12)),
        (2001, 202, date(2026, 1, 10), date(2026, 2, 1)),
    )
    return sa.union_all(
        *(
            sa.select(
                sa.literal(episode_id).label("episode_id"),
                sa.literal(person_id).label("person_id"),
                sa.literal(32533).label("episode_concept_id"),
                sa.literal(f"episode-{episode_id}").label("episode_label"),
                sa.literal(start).label("episode_start_date"),
                sa.literal(end).label("episode_end_date"),
            )
            for episode_id, person_id, start, end in rows
        )
    ).cte("episodes")


def _links(*rows: tuple[int, int, int]) -> sa.CTE:
    return sa.union_all(
        *(
            sa.select(
                sa.literal(episode_id).label("episode_id"),
                sa.literal(event_id).label("event_id"),
                sa.literal(field_concept_id).label(
                    "episode_event_field_concept_id"
                ),
            )
            for episode_id, event_id, field_concept_id in rows
        )
    ).cte("episode_events")


def _attached(
    events: sa.CTE,
    links: sa.CTE,
    *,
    policy: EpisodeAttachmentPolicy = EVENT_CONSTRUCT_ATTACHMENT_POLICY,
) -> sa.Subquery:
    return attach_to_condition_episode(
        events,
        event_id_col=events.c.event_id,
        date_col=events.c.event_date,
        person_col=events.c.person_id,
        name="attached_events",
        policy=policy,
        episodes=_episodes(),
        episode_events=links,
    )


def _rows(statement: sa.Subquery) -> list[sa.RowMapping]:
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


def test_unlinked_fallback_retains_each_overlapping_episode_once():
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

    assert {(row["event_id"], row["episode_id"]) for row in rows} == {
        (8, 1001),
        (8, 1002),
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
    )


def test_direct_legacy_attachment_helpers_warn_and_compile():
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

    with pytest.warns(DeprecationWarning, match="compatibility wrapper"):
        explicit = attach_to_condition_episode_via_episode_event(
            events,
            event_id_col=events.c.event_id,
            date_col=events.c.event_date,
            name="legacy_explicit_helper",
            episodes=episodes,
            episode_events=_links(
                (1001, 7, MEASUREMENT.event_field_concept_id),
            ),
        )
    with pytest.warns(DeprecationWarning, match="compatibility wrapper"):
        fallback = attach_to_condition_episode_by_time_window(
            events,
            date_col=events.c.event_date,
            person_col=events.c.person_id,
            name="legacy_window_helper",
            episodes=episodes,
        )

    assert _rows(explicit)[0]["episode_id"] == 1001
    assert {row["episode_id"] for row in _rows(fallback)} == {1001, 1002}
