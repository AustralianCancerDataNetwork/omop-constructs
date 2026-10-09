"""OC-1 compatibility and counterexample coverage for event attachment."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from omop_alchemy.cdm.model import Concept, Episode, Episode_Event, Measurement, Observation, Procedure_Occurrence
from omop_alchemy.cdm.model.clinical.event_metadata import clinical_event_model_spec
from omop_alchemy.toolkit.episodes.derivation import (
    EpisodeAttachmentPolicy,
    EpisodeWindowSpec,
    TemporalSelectionPolicy,
    TemporalSidePreference,
)
from omop_semantics.runtime.default_valuesets import runtime
from omop_constructs.alchemy.episodes.condition_episode_mv import ConditionEpisodeMV
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
    episode_relevant_window,
)


MEASUREMENT = clinical_event_model_spec(Measurement)
OBSERVATION = clinical_event_model_spec(Observation)
PROCEDURE = clinical_event_model_spec(Procedure_Occurrence)

EPISODE_OF_CARE = int(runtime.types.disease_episode_types.episode_of_care)
PROGRESSION = int(runtime.types.disease_episode_types.disease_progression)
METASTATIC = int(runtime.types.disease_episode_types.metastatic)

EpisodeRow = tuple[int, int, date, date | None, int] | tuple[int, int, date, date | None, int, int | None]

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
                "episode_end_date": end if end is not None else sa.cast(sa.null(), sa.Date()),
                "episode_parent_id": parent[0] if parent else sa.cast(sa.null(), sa.Integer()),
            }
            for episode_id, person_id, start, end, concept_id, *parent in rows
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
    window: EpisodeWindowSpec = EpisodeWindowSpec(),
) -> sa.Subquery:
    return attach_to_condition_episode(
        events,
        event_id_col=events.c.event_id,
        date_col=events.c.event_date,
        person_col=events.c.person_id,
        name="attached_events",
        policy=policy,
        window=window,
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

    # attachment_method and the private bounded end are consumed by the window.
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
        "_fallback_window_end",
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

def _child(concept_id: int, *, episode_id: int = 3002) -> EpisodeRow:
    return (episode_id, 101, date(2025, 6, 1), date(2025, 12, 31), concept_id, 3001)


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


@pytest.mark.parametrize("child_concept", [PROGRESSION, METASTATIC])
def test_ranked_fallback_chooses_the_episode_of_care_over_its_later_child(child_concept):
    attached = _attached(
        _events(_procedure(8, date(2025, 7, 1))),
        _no_links(),
        episodes=_episodes((ROOT, _child(child_concept))),
    )

    assert _pairs(attached) == {(8, 3001)}


def _unlinked_procedures(episodes: Sequence[EpisodeRow], *events: tuple) -> sa.Subquery:
    """Shared synthetic setup for unlinked episode-window counterexamples."""
    return _attached(_events(*events), _no_links(), episodes=_episodes(episodes))


@pytest.mark.parametrize("episode_rows", [
    (
        (3001, 101, date(2024, 1, 10), root_end, EPISODE_OF_CARE),
        (3003, 101, date(2025, 3, 1), root_end, EPISODE_OF_CARE),
        _child(PROGRESSION),
    )
    for root_end in (date(2025, 12, 31), None)
], ids=["closed-roots", "extension-overlap"])
def test_ranked_fallback_compares_episode_of_care_starts_only(episode_rows):
    attached = _unlinked_procedures(episode_rows, _procedure(8, date(2025, 7, 1)))

    # The older root's child started most recently, but it does not compete,
    # and it is not remapped to its root either.
    assert _pairs(attached) == {(8, 3003)}
    assert _pairs(episode_relevant_window(attached)) == {(8, 3003)}


def test_event_inside_only_a_child_window_attaches_to_the_root_by_date():
    short_root = (3001, 101, date(2025, 1, 10), date(2025, 3, 31), EPISODE_OF_CARE)
    attached = _unlinked_procedures(
        (short_root, _child(PROGRESSION)), _procedure(8, date(2025, 8, 1))
    )

    assert _pairs(attached) == {(8, 3001)}
    assert _pairs(episode_relevant_window(attached)) == {(8, 3001)}


@pytest.mark.parametrize("child_concept", [PROGRESSION, METASTATIC])
def test_open_root_extends_to_the_latest_child_window_end(child_concept):
    root = (3001, 101, date(2024, 1, 10), None, EPISODE_OF_CARE)
    child = (3002, 101, date(2025, 6, 1), None, child_concept, 3001)
    attached = _unlinked_procedures(
        (root, child), _procedure(8, date(2026, 6, 1)), _procedure(9, date(2026, 6, 2))
    )
    rows = _rows(episode_relevant_window(attached))
    assert {(row["event_id"], row["episode_id"]) for row in rows} == {(8, 3001)}
    assert rows[0]["episode_end_date"] is None  # output retains the recorded end


def test_root_end_that_already_covers_its_child_is_not_shortened():
    root = (3001, 101, date(2025, 1, 10), date(2026, 12, 31), EPISODE_OF_CARE)
    attached = _unlinked_procedures(
        (root, _child(PROGRESSION)), _procedure(8, date(2026, 12, 31)), _procedure(9, date(2027, 1, 1))
    )
    assert _pairs(episode_relevant_window(attached)) == {(8, 3001)}


def test_extension_uses_latest_child_and_ignores_other_people_and_treatment():
    root = (3001, 101, date(2025, 1, 10), date(2025, 3, 31), EPISODE_OF_CARE)
    later_child = (3004, 101, date(2025, 7, 1), date(2026, 1, 31), METASTATIC, 3001)
    other_person = (3005, 202, date(2025, 7, 1), date(2028, 1, 1), PROGRESSION, 3001)
    treatment = (3006, 101, date(2025, 7, 1), date(2028, 1, 1), 32531, 3001)
    attached = _attached(
        _events(_procedure(8, date(2026, 1, 31)), _procedure(9, date(2026, 2, 1))),
        _no_links(), episodes=_episodes((root, _child(PROGRESSION), later_child, other_person, treatment)),
    )
    assert _pairs(episode_relevant_window(attached)) == {(8, 3001)}


def test_extension_uses_the_callers_open_end_horizon_and_bound_inclusivity():
    root = (3001, 101, date(2025, 1, 10), None, EPISODE_OF_CARE)
    child = (3002, 101, date(2025, 6, 1), None, PROGRESSION, 3001)
    attached = _attached(
        _events(_procedure(8, date(2025, 6, 30)), _procedure(9, date(2025, 7, 1))),
        _no_links(), episodes=_episodes((root, child)),
        window=EpisodeWindowSpec(open_end_fallback_days=30, include_upper_bound=False),
    )
    assert _pairs(episode_relevant_window(attached)) == {(8, 3001)}


def test_all_in_window_retains_the_original_root_window():
    root = (3001, 101, date(2025, 1, 10), date(2025, 3, 31), EPISODE_OF_CARE)
    attached = _attached(
        _events(_procedure(8, date(2025, 8, 1))), _no_links(),
        episodes=_episodes((root, _child(PROGRESSION))),
        policy=EpisodeAttachmentPolicy.explicit_first_all_in_window,
    )
    assert _pairs(episode_relevant_window(attached)) == {(8, 3002)}


@pytest.mark.parametrize(
    "policy",
    [
        EpisodeAttachmentPolicy.explicit_only,
        EpisodeAttachmentPolicy.explicit_first_ranked,
        EpisodeAttachmentPolicy.explicit_first_all_in_window,
    ],
)
@pytest.mark.parametrize("root_end", [date(2025, 3, 31), date(2025, 12, 31)])
def test_valid_explicit_links_to_a_child_are_kept(policy, root_end):
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
        episodes=_episodes(((*ROOT[:3], root_end, ROOT[4]), _child(PROGRESSION))),
    )

    assert _pairs(episode_relevant_window(attached)) == {(8, 3002), (9, 3002), (10, 3001), (10, 3002)}


def test_all_in_window_fallback_still_includes_disease_children():
    attached = _attached(
        _events(_procedure(8, date(2025, 7, 1))),
        _no_links(),
        policy=EpisodeAttachmentPolicy.explicit_first_all_in_window,
        episodes=_episodes((ROOT, _child(PROGRESSION))),
    )

    assert _pairs(attached) == {(8, 3001), (8, 3002)}


@pytest.mark.parametrize("child_end", [None, date(2026, 6, 1)])
@pytest.mark.parametrize(("factory", "spec", "event_model"), [
    (measurement_attached_to_condition_episode, MEASUREMENT, Measurement),
    (observation_attached_to_condition_episode, OBSERVATION, Observation),
    (procedure_attached_to_condition_episode, PROCEDURE, Procedure_Occurrence),
])
def test_default_source_extends_with_the_cdm_hierarchy(child_end, factory, spec, event_model):
    """Exercise the real default source, whose public projection has no parent ID."""
    metadata = sa.MetaData()
    episode_columns = ("episode_id", "person_id", "episode_concept_id",
                       "episode_parent_id", "episode_start_date", "episode_end_date")
    cdm = sa.Table(
        Episode.__tablename__, metadata,
        *(sa.Column(key, Episode.__table__.c[key].type) for key in episode_columns),
    )
    projected = sa.Table(
        ConditionEpisodeMV.__tablename__, metadata,
        *(sa.Column(column.key, column.type) for column in ConditionEpisodeMV.__table__.c),
    )
    source = sa.Table(
        event_model.__tablename__, metadata,
        *(sa.Column(c.key, c.type) for c in event_model.__table__.c),
    )
    for model in (Concept, Episode_Event):
        sa.Table(model.__tablename__, metadata,
                 *(sa.Column(c.key, c.type) for c in model.__table__.c))
    engine = sa.create_engine("sqlite://")
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(cdm.insert(), [
            dict(episode_id=3001, person_id=101, episode_concept_id=EPISODE_OF_CARE,
                 episode_parent_id=None, episode_start_date=date(2024, 1, 10), episode_end_date=None),
            dict(episode_id=3002, person_id=101, episode_concept_id=PROGRESSION,
                 episode_parent_id=3001, episode_start_date=date(2025, 6, 1), episode_end_date=child_end),
        ])
        connection.execute(projected.insert().from_select(
            [column.key for column in projected.c],
            sa.select(cdm.c.episode_id, cdm.c.person_id, cdm.c.episode_concept_id,
                      sa.literal("diagnosis"), cdm.c.episode_start_date, cdm.c.episode_end_date),
        ))
        connection.execute(source.insert(), [
            {spec.event_id_column: event_id, "person_id": 101,
             spec.event_date_column: event_date, spec.event_concept_id_column: 900_001}
            for event_id, event_date in [(8, date(2026, 6, 1)), (9, date(2026, 6, 2))]
        ])
        attached = factory(name="default_attached", policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY)
        windowed = episode_relevant_window(attached)
        assert "attachment_method" not in windowed.c
        assert "_fallback_window_end" not in windowed.c
        rows = connection.execute(sa.select(windowed)).mappings().all()
    assert [(row["event_id"], row["episode_id"], row["episode_end_date"]) for row in rows] == [(8, 3001, None)]



def test_unextended_ranked_root_keeps_the_legacy_window_override():
    root = (3001, 101, date(2025, 1, 10), None, EPISODE_OF_CARE)
    attached = _attached(
        _events(_procedure(8, date(2025, 2, 9)), _procedure(9, date(2025, 2, 10))),
        _no_links(), episodes=_episodes((root,)),
    )
    assert _pairs(episode_relevant_window(attached, max_days_post=30)) == {(8, 3001)}



def test_carrying_window_bound_does_not_multiply_explicit_root_links():
    events = _events(_procedure(8, date(2025, 7, 1)))
    links = _links((3001, 8, PROCEDURE.event_field_concept_id))
    episodes = _episodes((ROOT, ROOT, _child(PROGRESSION)))
    ranked = _attached(events, links, episodes=episodes)
    explicit = _attached(events, links, episodes=episodes,
                         policy=EpisodeAttachmentPolicy.explicit_only)
    assert len(_rows(ranked)) == len(_rows(explicit))


@pytest.mark.parametrize("age,offset,expected", [(730, 40, 2), (195, 17, 1), (365, 60, 2), (364, 60, 1), (730, 61, 1)])
@pytest.mark.parametrize("event_spec", [MEASUREMENT, OBSERVATION, PROCEDURE])
def test_default_attachment_applies_narrow_upcoming_preference(age, offset, expected, event_spec):
    from datetime import timedelta

    anchor = date(2026, 1, 20)
    episodes = _episodes((
        (1, 101, anchor - timedelta(days=age), date(2026, 12, 31), EPISODE_OF_CARE),
        (2, 101, anchor + timedelta(days=offset), date(2026, 12, 31), EPISODE_OF_CARE),
        (3, 101, anchor - timedelta(days=10), date(2026, 12, 31), PROGRESSION, 1),
    ))
    events = _events((event_spec.event_source_table, event_spec.event_field_concept_id, 101, anchor, 8))
    rows = _rows(_attached(events, _links((999, 8, event_spec.event_field_concept_id)), episodes=episodes))
    assert [(r["event_id"], r["episode_id"], r["attachment_method"]) for r in rows] == [(8, expected, "fallback")]


def test_upcoming_preference_uses_latest_started_root_and_preserves_custom_ranking():
    episodes = _episodes((
        (1, 101, date(2023, 1, 1), date(2026, 12, 31), EPISODE_OF_CARE),
        (2, 101, date(2025, 7, 9), date(2026, 12, 31), EPISODE_OF_CARE),
        (3, 101, date(2026, 2, 6), date(2026, 12, 31), EPISODE_OF_CARE),
    ))
    events = _events((PROCEDURE.event_source_table, PROCEDURE.event_field_concept_id, 101, date(2026, 1, 20), 8))
    links = _links((999, 8, PROCEDURE.event_field_concept_id))
    assert [r["episode_id"] for r in _rows(_attached(events, links, episodes=episodes))] == [2]
    old_and_upcoming = sa.select(episodes).where(episodes.c.episode_id != 2).subquery()
    custom = attach_to_condition_episode(
        events, event_id_col=events.c.event_id, date_col=events.c.event_date,
        person_col=events.c.person_id, name="custom", episodes=old_and_upcoming,
        episode_events=links, ranking=EVENT_CONSTRUCT_ATTACHMENT_RANKING,
        policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    )
    assert [r["episode_id"] for r in _rows(custom)] == [1]


def test_upcoming_preference_keeps_explicit_old_or_child_links():
    episodes = _episodes((
        (1, 101, date(2023, 1, 1), date(2026, 12, 31), EPISODE_OF_CARE),
        (2, 101, date(2026, 3, 1), date(2026, 12, 31), EPISODE_OF_CARE),
        (3, 101, date(2025, 1, 1), date(2025, 2, 1), PROGRESSION, 1),
    ))
    events = _events((PROCEDURE.event_source_table, PROCEDURE.event_field_concept_id, 101, date(2026, 1, 20), 8))
    rows = _rows(_attached(events, _links((1, 8, PROCEDURE.event_field_concept_id), (3, 8, PROCEDURE.event_field_concept_id)), episodes=episodes))
    assert {(r["episode_id"], r["attachment_method"]) for r in rows} == {(1, "explicit"), (3, "explicit")}


# Synthetic counterparts of all eight real-data patterns in bench/make_bench.py.
_BENCH_ATTACHMENT_CASES = [('root without end date, later child',
  [(90000101, 32533, None, '2020-01-10', None), (90000102, 32949, 90000101, '2021-06-01', None)],
  'measurement',
  900000001,
  '2020-06-01',
  None,
  90000101),
 ('root without end date, later child',
  [(90000101, 32533, None, '2020-01-10', None), (90000102, 32949, 90000101, '2021-06-01', None)],
  'measurement',
  900000002,
  '2021-09-01',
  None,
  90000101),
 ('root without end date, later child',
  [(90000101, 32533, None, '2020-01-10', None), (90000102, 32949, 90000101, '2021-06-01', None)],
  'measurement',
  900000003,
  '2022-09-01',
  None,
  None),
 ('gap between root and child windows',
  [(90000201, 32533, None, '2018-03-01', None), (90000202, 32949, 90000201, '2021-01-01', None)],
  'measurement',
  900000011,
  '2019-10-01',
  None,
  90000201),
 ('near-synchronous primaries',
  [(90000301, 32533, None, '2022-01-10', None), (90000302, 32533, None, '2022-02-20', None)],
  'procedure',
  900000021,
  '2022-02-01',
  None,
  90000301),
 ('pre-diagnosis referral beside a long-running older cancer',
  [(90000401, 32533, None, '2019-01-15', None),
   (90000402, 32949, 90000401, '2020-06-01', None),
   (90000403, 32533, None, '2021-03-01', None)],
  'observation',
  900000031,
  '2021-01-20',
  None,
  90000403),
 ('late ECOG during progression',
  [(90000501, 32533, None, '2019-05-01', None), (90000502, 32949, 90000501, '2021-01-01', None)],
  'measurement',
  900000041,
  '2021-09-01',
  None,
  90000501),
 ('explicit link to a child outside every window',
  [(90000601, 32533, None, '2020-01-10', None), (90000602, 32949, 90000601, '2020-09-01', None)],
  'procedure',
  900000051,
  '2024-01-01',
  90000602,
  90000602),
 ('demographic observation, two primaries',
  [(90000701, 32533, None, '2018-01-01', '2020-01-01'), (90000702, 32533, None, '2019-06-01', None)],
  'observation',
  900000061,
  '2019-08-01',
  None,
  90000702),
 ('upcoming diagnosis soon, older cancer recent',
  [(90000801, 32533, None, '2022-01-01', None), (90000802, 32533, None, '2022-08-01', None)],
  'measurement',
  900000071,
  '2022-07-15',
  None,
  90000801)]


@pytest.mark.parametrize("scenario,episode_rows,source,event_id,event_date,link,expected", _BENCH_ATTACHMENT_CASES)
def test_q4_mirrors_every_synthetic_bench_scenario(scenario, episode_rows, source, event_id, event_date, link, expected):
    spec = {"measurement": MEASUREMENT, "observation": OBSERVATION, "procedure": PROCEDURE}[source]
    episodes = _episodes(tuple(
        (ep, 101, date.fromisoformat(start), date.fromisoformat(end) if end else None, concept, parent)
        for ep, concept, parent, start, end in episode_rows
    ))
    events = _events((spec.event_source_table, spec.event_field_concept_id, 101, date.fromisoformat(event_date), event_id))
    links = _links((link, event_id, spec.event_field_concept_id)) if link else _no_links()
    attached = _attached(events, links, episodes=episodes)
    assert _pairs(episode_relevant_window(attached)) == (set() if expected is None else {(event_id, expected)}), scenario
