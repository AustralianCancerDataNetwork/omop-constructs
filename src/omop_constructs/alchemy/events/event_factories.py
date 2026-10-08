"""Clinical-event query factories backed by OMOP Alchemy attachment contracts."""

from __future__ import annotations

import warnings
from typing import Any, Iterable, Sequence, TypeAlias

import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_semantics.runtime.default_valuesets import runtime  # type: ignore
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.selectable import FromClause, SelectBase

from omop_alchemy.cdm.model import Concept, Measurement, Observation, Procedure_Occurrence
from omop_alchemy.cdm.model.clinical.event_metadata import clinical_event_model_spec
from omop_alchemy.cdm.model.structural import Episode_Event
from omop_alchemy.toolkit.core.events import (
    ClinicalEventColumn,
    canonical_event_projection,
)
from omop_alchemy.toolkit.episodes.derivation import (
    ATTACHMENT_METHOD,
    EpisodeAttachmentMethod,
    EpisodeAttachmentPolicy,
    EpisodeWindowSpec,
    TemporalRankingSpec,
    TemporalSelectionPolicy,
    TemporalSidePreference,
    episode_attachment_queries,
    episode_window_predicate,
)

from omop_constructs.alchemy.episodes.condition_episode_mv import ConditionEpisodeMV


SQLExpr: TypeAlias = ColumnElement[Any] | InstrumentedAttribute[Any]
AttachmentSource: TypeAlias = FromClause | SelectBase

DEFAULT_EPISODE_WINDOW_DAYS_POST = 365
DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90
DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365

EVENT_CONSTRUCT_ATTACHMENT_POLICY = EpisodeAttachmentPolicy.explicit_first_ranked
EVENT_CONSTRUCT_ATTACHMENT_RANKING = TemporalRankingSpec(
    policy=TemporalSelectionPolicy.nearest,
    stable_id_column="episode_id",
    side_preference=TemporalSidePreference.on_or_before_anchor,
)

modifier_concept = so.aliased(Concept, name="modifier_concept")
procedure_concept = so.aliased(Concept, name="procedure_concept")
observation_concept = so.aliased(Concept, name="observation_concept")

_CANONICAL_REQUIRED = tuple(
    str(column) for column in ClinicalEventColumn.required_columns()
)
_COMPATIBILITY_HIDDEN_EVENT_COLUMNS = frozenset(
    {
        str(ClinicalEventColumn.event_datetime),
        str(ClinicalEventColumn.event_field_concept_id),
        str(ClinicalEventColumn.event_source_table),
    }
)
_EPISODE_COMPATIBILITY_COLUMNS = (
    "episode_concept_id",
    "episode_label",
    "episode_start_date",
    "episode_end_date",
)


def _as_from_clause(source: AttachmentSource, *, name: str) -> FromClause:
    if isinstance(source, SelectBase):
        return source.subquery(name)
    if isinstance(source, FromClause):
        return source
    raise TypeError(f"{name} must be a SQLAlchemy Select or FromClause")


def _episode_source(source: AttachmentSource | type[ConditionEpisodeMV]) -> FromClause:
    if isinstance(source, type):
        return sa.select(
            source.episode_id,
            source.person_id,
            source.episode_concept_id,
            source.episode_label,
            source.episode_start_date,
            source.episode_end_date,
        ).subquery("condition_episodes")
    return _as_from_clause(source, name="condition_episodes")


def _require_columns(source: FromClause, names: Sequence[str], *, role: str) -> None:
    missing = tuple(name for name in names if name not in source.c)
    if missing:
        raise ValueError(f"{role} is missing required columns: {', '.join(missing)}")


def _canonicalize_legacy_events(
    source: AttachmentSource,
    *,
    event_id_col: ColumnElement[Any],
    date_col: ColumnElement[Any],
    person_col: ColumnElement[Any],
    event_model: type[Any],
    name: str,
) -> FromClause:
    event_source = _as_from_clause(source, name=f"{name}_events")
    if all(column in event_source.c for column in _CANONICAL_REQUIRED):
        return event_source

    event_id_name = event_id_col.key
    date_name = date_col.key
    person_name = person_col.key
    _require_columns(
        event_source,
        (person_name, event_id_name, date_name, "event_concept_id"),
        role="event source",
    )
    spec = clinical_event_model_spec(event_model)
    consumed = {person_name, event_id_name, date_name, "event_concept_id"}
    extras = tuple(column for column in event_source.c if column.key not in consumed)

    # Populate cross-table identity from the model metadata so the shared
    # builder can validate discriminators, apply precedence, admit candidates,
    # and remove duplicate relationships.
    return sa.select(
        event_source.c[person_name].label(str(ClinicalEventColumn.person_id)),
        event_source.c[event_id_name].label(str(ClinicalEventColumn.event_id)),
        event_source.c[date_name].label(str(ClinicalEventColumn.event_date)),
        sa.cast(sa.null(), sa.DateTime()).label(
            str(ClinicalEventColumn.event_datetime)
        ),
        event_source.c.event_concept_id.label(
            str(ClinicalEventColumn.event_concept_id)
        ),
        sa.literal(spec.event_field_concept_id).label(
            str(ClinicalEventColumn.event_field_concept_id)
        ),
        sa.literal(spec.event_source_table).label(
            str(ClinicalEventColumn.event_source_table)
        ),
        *extras,
    ).subquery(f"{name}_canonical_events")


def _empty_episode_events() -> FromClause:
    return (
        sa.select(
            sa.cast(sa.null(), sa.Integer()).label("episode_id"),
            sa.cast(sa.null(), sa.Integer()).label("event_id"),
            sa.cast(sa.null(), sa.Integer()).label(
                "episode_event_field_concept_id"
            ),
        )
        .where(sa.false())
        .subquery("empty_episode_events")
    )


def _fallback_episodes(
    episodes: FromClause,
    policy: EpisodeAttachmentPolicy,
) -> FromClause | None:
    """Choose the episodes a policy may attach unlinked events to by date.

    Ranked fallback chooses among episodes of care only. A progression or
    metastatic episode starts on or after its episode of care, so admitting it
    would let a nested diagnosis win the ranking, and cohort rows are keyed on
    the episode of care. All-in-window fallback keeps every disease episode, and
    explicit-only attachment has no fallback stage. Explicit links are validated
    against the full source in every case.
    """
    if not policy.uses_fallback:
        return None
    if not policy.requires_fallback_ranking:
        return episodes
    return (
        sa.select(episodes)
        .where(
            episodes.c.episode_concept_id
            == runtime.types.disease_episode_types.episode_of_care  # type: ignore[attr-defined]
        )
        .subquery("episode_of_care_candidates")
    )


def _legacy_attachment_result(
    events: FromClause,
    episodes: FromClause,
    *,
    policy: EpisodeAttachmentPolicy,
    episode_events: AttachmentSource | type[Episode_Event],
    ranking: TemporalRankingSpec | None,
    window: EpisodeWindowSpec,
    name: str,
) -> sa.Subquery:
    _require_columns(
        episodes,
        (
            "episode_id",
            "person_id",
            *_EPISODE_COMPATIBILITY_COLUMNS,
        ),
        role="condition episode source",
    )
    attachments = episode_attachment_queries(
        events,
        explicit_episodes=episodes,
        fallback_episodes=_fallback_episodes(episodes, policy),
        episode_events=episode_events,
        policy=policy,
        ranking=ranking,
        window=window,
    ).attachments.subquery(f"{name}_canonical_attachments")
    event_columns = tuple(
        column.key
        for column in events.c
        if column.key not in _COMPATIBILITY_HIDDEN_EVENT_COLUMNS
    )

    return (
        sa.select(
            *(attachments.c[column] for column in event_columns),
            episodes.c.episode_id,
            *(episodes.c[column] for column in _EPISODE_COMPATIBILITY_COLUMNS),
            (
                attachments.c.event_date - episodes.c.episode_start_date
            ).label("episode_delta_days"),
            attachments.c[ATTACHMENT_METHOD],
        )
        .join(episodes, episodes.c.episode_id == attachments.c.episode_id)
        .subquery(name=name)
    )


def _resolve_attachment_policy(
    *,
    policy: EpisodeAttachmentPolicy | None,
    prefer_explicit_link: bool | None,
    stacklevel: int,
) -> EpisodeAttachmentPolicy:
    if policy is not None and prefer_explicit_link is not None:
        raise ValueError("pass policy or prefer_explicit_link, not both")
    if policy is not None:
        return policy

    warnings.warn(
        "prefer_explicit_link and the implicit attachment policy are deprecated; "
        "pass an EpisodeAttachmentPolicy explicitly",
        DeprecationWarning,
        stacklevel=stacklevel,
    )
    if prefer_explicit_link is not None:
        return (
            EpisodeAttachmentPolicy.explicit_first_all_in_window
            if prefer_explicit_link
            else EpisodeAttachmentPolicy.explicit_only
        )
    return EVENT_CONSTRUCT_ATTACHMENT_POLICY


def attach_to_condition_episode_via_episode_event(
    base_event_subq: AttachmentSource,
    *,
    event_id_col: ColumnElement[Any],
    date_col: ColumnElement[Any],
    name: str,
    person_col: ColumnElement[Any] | None = None,
    event_model: type[Any] = Measurement,
    episodes: AttachmentSource | type[ConditionEpisodeMV] = ConditionEpisodeMV,
    episode_events: AttachmentSource | type[Episode_Event] = Episode_Event,
) -> sa.Subquery:
    """Attach only valid explicit links through the shared alchemy builder."""
    warnings.warn(
        "attach_to_condition_episode_via_episode_event is deprecated; use "
        "attach_to_condition_episode(..., policy=explicit_only)",
        DeprecationWarning,
        stacklevel=2,
    )
    source = _as_from_clause(base_event_subq, name=f"{name}_legacy_events")
    canonical = _canonicalize_legacy_events(
        source,
        event_id_col=event_id_col,
        date_col=date_col,
        person_col=source.c.person_id if person_col is None else person_col,
        event_model=event_model,
        name=name,
    )
    return _legacy_attachment_result(
        canonical,
        _episode_source(episodes),
        policy=EpisodeAttachmentPolicy.explicit_only,
        episode_events=episode_events,
        ranking=None,
        window=EpisodeWindowSpec(),
        name=name,
    )


def attach_to_condition_episode_by_time_window(
    base_event_subq: AttachmentSource,
    *,
    date_col: ColumnElement[Any],
    person_col: ColumnElement[Any],
    name: str,
    event_id_col: ColumnElement[Any] | None = None,
    event_model: type[Any] = Measurement,
    episodes: AttachmentSource | type[ConditionEpisodeMV] = ConditionEpisodeMV,
    window: EpisodeWindowSpec = EpisodeWindowSpec(),
) -> sa.Subquery:
    """Attach every eligible in-window episode without an explicit-link source."""
    warnings.warn(
        "attach_to_condition_episode_by_time_window is deprecated; use "
        "attach_to_condition_episode with a named fallback policy",
        DeprecationWarning,
        stacklevel=2,
    )
    source = _as_from_clause(base_event_subq, name=f"{name}_legacy_events")
    canonical = _canonicalize_legacy_events(
        source,
        event_id_col=source.c.event_id if event_id_col is None else event_id_col,
        date_col=date_col,
        person_col=person_col,
        event_model=event_model,
        name=name,
    )
    return _legacy_attachment_result(
        canonical,
        _episode_source(episodes),
        policy=EpisodeAttachmentPolicy.explicit_first_all_in_window,
        episode_events=_empty_episode_events(),
        ranking=None,
        window=window,
        name=name,
    )


def attach_to_condition_episode(
    base_event_subq: AttachmentSource,
    *,
    event_id_col: ColumnElement[Any],
    date_col: ColumnElement[Any],
    person_col: ColumnElement[Any],
    name: str,
    policy: EpisodeAttachmentPolicy | None = None,
    ranking: TemporalRankingSpec | None = None,
    window: EpisodeWindowSpec = EpisodeWindowSpec(),
    prefer_explicit_link: bool | None = None,
    event_model: type[Any] = Measurement,
    episodes: AttachmentSource | type[ConditionEpisodeMV] = ConditionEpisodeMV,
    episode_events: AttachmentSource | type[Episode_Event] = Episode_Event,
) -> sa.Subquery:
    """Attach events using a named precedence and cardinality policy.

    The default policy preserves valid explicit relationships. Events without
    one are assigned to a single eligible episode, preferring episodes that
    have started, then the nearest start, then the lowest episode identifier.
    """
    resolved_policy = _resolve_attachment_policy(
        policy=policy,
        prefer_explicit_link=prefer_explicit_link,
        stacklevel=2,
    )
    resolved_ranking = (
        EVENT_CONSTRUCT_ATTACHMENT_RANKING
        if resolved_policy is EVENT_CONSTRUCT_ATTACHMENT_POLICY and ranking is None
        else ranking
    )
    canonical = _canonicalize_legacy_events(
        base_event_subq,
        event_id_col=event_id_col,
        date_col=date_col,
        person_col=person_col,
        event_model=event_model,
        name=name,
    )
    return _legacy_attachment_result(
        canonical,
        _episode_source(episodes),
        policy=resolved_policy,
        episode_events=episode_events,
        ranking=resolved_ranking,
        window=window,
        name=name,
    )


def _canonical_event_core(
    model: type[Any],
    *,
    concept_model: Any,
    concept_column: ColumnElement[Any],
    concept_ids: Iterable[int] | None,
    include_cols: Sequence[SQLExpr],
    name: str,
    unlinked_predicate: ColumnElement[bool] | None = None,
    fixed_extras: Sequence[SQLExpr] = (),
) -> sa.Subquery:
    query = (
        canonical_event_projection(model, include_values=False)
        .add_columns(
            concept_model.concept_name.label("event_label"),
            *fixed_extras,
            *include_cols,
        )
        .join(
            concept_model,
            concept_model.concept_id == concept_column,
            isouter=True,
        )
    )
    if unlinked_predicate is not None:
        query = query.where(unlinked_predicate)
    if concept_ids is not None:
        query = query.where(concept_column.in_(list(concept_ids)))
    return query.subquery(name=name)


def _legacy_event_core(canonical: FromClause, *, name: str) -> sa.Subquery:
    return sa.select(
        *(
            column
            for column in canonical.c
            if column.key not in _COMPATIBILITY_HIDDEN_EVENT_COLUMNS
        )
    ).subquery(name=name)


def procedure_event_core(
    *,
    concept_ids: Iterable[int] | None = None,
    name: str = "procedure_core",
    include_cols: Sequence[SQLExpr] = (),
) -> sa.Subquery:
    """Return procedure-event columns from a canonical projection."""
    canonical = _canonical_event_core(
        Procedure_Occurrence,
        concept_model=procedure_concept,
        concept_column=Procedure_Occurrence.procedure_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_canonical",
    )
    return _legacy_event_core(canonical, name=name)


def procedure_attached_to_condition_episode(
    *,
    concept_ids: Iterable[int] | None = None,
    include_cols: Sequence[SQLExpr] = (),
    name: str,
    policy: EpisodeAttachmentPolicy | None = None,
    ranking: TemporalRankingSpec | None = None,
    prefer_explicit_link: bool | None = None,
) -> sa.Subquery:
    canonical = _canonical_event_core(
        Procedure_Occurrence,
        concept_model=procedure_concept,
        concept_column=Procedure_Occurrence.procedure_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_core",
    )
    return attach_to_condition_episode(
        canonical,
        event_id_col=canonical.c.event_id,
        date_col=canonical.c.event_date,
        person_col=canonical.c.person_id,
        name=name,
        policy=policy,
        ranking=ranking,
        prefer_explicit_link=prefer_explicit_link,
        event_model=Procedure_Occurrence,
    )


def measurement_event_core(
    *,
    concept_ids: Iterable[int] | None = None,
    name: str = "measurement_core",
    include_cols: Sequence[SQLExpr] = (),
    unlinked_only: bool = True,
) -> sa.Subquery:
    """Return measurement-event columns from a canonical projection."""
    canonical = _canonical_event_core(
        Measurement,
        concept_model=modifier_concept,
        concept_column=Measurement.measurement_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_canonical",
        unlinked_predicate=(
            Measurement.modifier_of_event_id.is_(None) if unlinked_only else None
        ),
    )
    return _legacy_event_core(canonical, name=name)


def measurement_attached_to_condition_episode(
    *,
    concept_ids: Iterable[int] | None = None,
    include_cols: Sequence[SQLExpr] = (),
    name: str,
    unlinked_only: bool = True,
    policy: EpisodeAttachmentPolicy | None = None,
    ranking: TemporalRankingSpec | None = None,
    prefer_explicit_link: bool | None = None,
) -> sa.Subquery:
    canonical = _canonical_event_core(
        Measurement,
        concept_model=modifier_concept,
        concept_column=Measurement.measurement_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_core",
        unlinked_predicate=(
            Measurement.modifier_of_event_id.is_(None) if unlinked_only else None
        ),
    )
    return attach_to_condition_episode(
        canonical,
        event_id_col=canonical.c.event_id,
        date_col=canonical.c.event_date,
        person_col=canonical.c.person_id,
        name=name,
        policy=policy,
        ranking=ranking,
        prefer_explicit_link=prefer_explicit_link,
        event_model=Measurement,
    )


def observation_event_core(
    *,
    concept_ids: Iterable[int] | None = None,
    name: str = "observation_core",
    include_cols: Sequence[SQLExpr] = (),
    unlinked_only: bool = True,
) -> sa.Subquery:
    """Return observation-event columns from a canonical projection."""
    canonical = _canonical_event_core(
        Observation,
        concept_model=observation_concept,
        concept_column=Observation.observation_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_canonical",
        unlinked_predicate=(
            Observation.observation_event_id.is_(None) if unlinked_only else None
        ),
        fixed_extras=(
            Observation.value_as_concept_id.label("value_concept_id"),
            Observation.qualifier_concept_id.label("qualifier_concept_id"),
        ),
    )
    return _legacy_event_core(canonical, name=name)


def observation_attached_to_condition_episode(
    *,
    concept_ids: Iterable[int] | None = None,
    include_cols: Sequence[SQLExpr] = (),
    name: str,
    unlinked_only: bool = True,
    policy: EpisodeAttachmentPolicy | None = None,
    ranking: TemporalRankingSpec | None = None,
    prefer_explicit_link: bool | None = None,
) -> sa.Subquery:
    canonical = _canonical_event_core(
        Observation,
        concept_model=observation_concept,
        concept_column=Observation.observation_concept_id,
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_core",
        unlinked_predicate=(
            Observation.observation_event_id.is_(None) if unlinked_only else None
        ),
        fixed_extras=(
            Observation.value_as_concept_id.label("value_concept_id"),
            Observation.qualifier_concept_id.label("qualifier_concept_id"),
        ),
    )
    return attach_to_condition_episode(
        canonical,
        event_id_col=canonical.c.event_id,
        date_col=canonical.c.event_date,
        person_col=canonical.c.person_id,
        name=name,
        policy=policy,
        ranking=ranking,
        prefer_explicit_link=prefer_explicit_link,
        event_model=Observation,
    )


def episode_relevant_window(
    starting_query: sa.Subquery,
    *,
    max_days_post: int = DEFAULT_EPISODE_WINDOW_DAYS_POST,
    max_days_prior: int = DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
    name: str | None = None,
) -> sa.Subquery:
    """Apply the event window and assign a refresh-local row number.

    Explicit relationships are recorded clinical assertions and remain valid
    outside the date window. Date-based relationships use the same window rule
    as candidate selection. ``attachment_method`` is consumed here because the
    materialized views select the remaining result columns positionally.
    """
    in_window = episode_window_predicate(
        starting_query.c.event_date,
        starting_query.c.episode_start_date,
        starting_query.c.episode_end_date,
        window=EpisodeWindowSpec(
            days_prior=max_days_prior, open_end_fallback_days=max_days_post
        ),
    )
    if ATTACHMENT_METHOD in starting_query.c:
        keep = sa.or_(
            starting_query.c[ATTACHMENT_METHOD]
            == str(EpisodeAttachmentMethod.explicit),
            in_window,
        )
    else:
        keep = in_window

    return (
        sa.select(
            sa.func.row_number().over().label("mv_id"),
            *(
                column
                for column in starting_query.c
                if column.key != ATTACHMENT_METHOD
            ),
        )
        .where(keep)
        .subquery(name=name or starting_query.name)
    )
