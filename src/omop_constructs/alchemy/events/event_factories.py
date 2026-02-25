import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.sql import ColumnElement
from typing import Iterable, Any, Sequence, Union
from omop_constructs.alchemy.episodes import ConditionEpisodeMV
from omop_alchemy.cdm.model.structural import Episode_Event
from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from omop_alchemy.cdm.model import Measurement, Concept, Procedure_Occurrence
from sqlalchemy.orm.attributes import InstrumentedAttribute

SQLExpr = Union[ColumnElement[Any], InstrumentedAttribute[Any]]

modifier_concept = so.aliased(Concept, name="modifier_concept")
procedure_concept = so.aliased(Concept, name="procedure_concept")

def attach_to_condition_episode_via_episode_event(
    base_event_subq: sa.Subquery,
    *,
    event_id_col: ColumnElement[Any],
    date_col: ColumnElement[Any],
    name: str,
) -> sa.Subquery:
    """
    Attach base events to ConditionEpisodeMV via Episode_Event links.
    """
    return (
        sa.select(
            *base_event_subq.c,
            ConditionEpisodeMV.episode_id,
            ConditionEpisodeMV.episode_concept_id,
            ConditionEpisodeMV.episode_label,
            ConditionEpisodeMV.episode_start_date,
            ConditionEpisodeMV.episode_end_date,
            (date_col - ConditionEpisodeMV.episode_start_date).label("episode_delta_days"),
        )
        .join(
            Episode_Event,
            sa.and_(
                Episode_Event.event_id == event_id_col,
                Episode_Event.episode_event_field_concept_id
                == runtime.modifiers.modifier_fields.measurement_id,
            ),
        )
        .join(
            ConditionEpisodeMV,
            ConditionEpisodeMV.episode_id == Episode_Event.episode_id,
        )
        .subquery(name=name)
    )   

def attach_to_condition_episode_by_time_window(
    base_event_subq: sa.Subquery,
    *,
    date_col: ColumnElement[Any],
    person_col: ColumnElement[Any],
    name: str,
) -> sa.Subquery:
    """
    Time-window attach to ConditionEpisodeMV (fallback when no Episode_Event link exists).
    """
    return (
        sa.select(
            *base_event_subq.c,

            ConditionEpisodeMV.episode_id,
            ConditionEpisodeMV.episode_concept_id,
            ConditionEpisodeMV.episode_label,
            ConditionEpisodeMV.episode_start_date,
            ConditionEpisodeMV.episode_end_date,
            (date_col - ConditionEpisodeMV.episode_start_date).label("episode_delta_days"),
        )
        .join(
            ConditionEpisodeMV,
            sa.and_(
                ConditionEpisodeMV.person_id == person_col,
                date_col >= ConditionEpisodeMV.episode_start_date,
                sa.or_(
                    ConditionEpisodeMV.episode_end_date == None,
                    date_col <= ConditionEpisodeMV.episode_end_date,
                ),
            ),
            isouter=True,
        )
        .subquery(name=name)
    )

def attach_to_condition_episode(
    base_event_subq: sa.Subquery,
    *,
    event_id_col: ColumnElement[Any],
    date_col: ColumnElement[Any],
    person_col: ColumnElement[Any],
    name: str,
    prefer_explicit_link: bool = True,
) -> sa.Subquery:
    """
    Attach events to ConditionEpisodeMV.
    
    Prefers explicit Episode_Event links; can fall back to time-window logic.
    """
    explicit = attach_to_condition_episode_via_episode_event(
        base_event_subq,
        date_col=date_col,
        event_id_col=event_id_col,
        name=f"{name}_explicit",
    )

    if not prefer_explicit_link:
        return explicit

    fallback = attach_to_condition_episode_by_time_window(
        base_event_subq,
        date_col=date_col,
        person_col=person_col,
        name=f"{name}_fallback",
    )

    return (
        sa.select(*explicit.c)
        .union_all(sa.select(*fallback.c))
        .subquery(name=name)
    )

def procedure_event_core(
    *,
    concept_ids: Iterable[int] | None = None,
    name: str = "procedure_core",
    include_cols: Sequence[SQLExpr] = (),
) -> sa.Subquery:
    """
    Minimal procedure event surface (person/id/date/concept/label + extras).
    """
    q = (
        sa.select(
            Procedure_Occurrence.person_id.label("person_id"),
            Procedure_Occurrence.procedure_occurrence_id.label("event_id"),
            Procedure_Occurrence.procedure_date.label("event_date"),
            Procedure_Occurrence.procedure_concept_id.label("event_concept_id"),
            procedure_concept.concept_name.label("event_label"),
            *include_cols,
        )
        .join(
            procedure_concept,
            procedure_concept.concept_id == Procedure_Occurrence.procedure_concept_id,
            isouter=True,
        )
    )

    if concept_ids is not None:
        q = q.where(Procedure_Occurrence.procedure_concept_id.in_(list(concept_ids)))

    return q.subquery(name=name)

def procedure_attached_to_condition_episode(
    *,
    concept_ids: Iterable[int] | None = None,
    include_cols: Sequence[SQLExpr] = (),
    name: str,
    prefer_explicit_link: bool = True,
) -> sa.Subquery:
    core = procedure_event_core(
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_core",
    )

    return attach_to_condition_episode(
        core,
        event_id_col=core.c.event_id,
        date_col=core.c.event_date,
        person_col=core.c.person_id,
        name=name,
        prefer_explicit_link=prefer_explicit_link,
    )


def measurement_event_core(
    *,
    concept_ids: Iterable[int] | None = None,
    name: str = "measurement_core",
    include_cols: Sequence[SQLExpr] = (),
    unlinked_only: bool = True,
) -> sa.Subquery:
    """
    Minimal measurement event surface (person/id/date/concept/label + extras).
    """
    q = (
        sa.select(
            Measurement.person_id.label("person_id"),
            Measurement.measurement_id.label("event_id"),
            Measurement.measurement_date.label("event_date"),
            Measurement.measurement_concept_id.label("event_concept_id"),
            modifier_concept.concept_name.label("event_label"),
            *include_cols,
        )
        .join(
            modifier_concept,
            modifier_concept.concept_id == Measurement.measurement_concept_id,
            isouter=True,
        )
    )

    if unlinked_only:
        q = q.where(Measurement.modifier_of_event_id == None)

    if concept_ids is not None:
        q = q.where(Measurement.measurement_concept_id.in_(list(concept_ids)))

    return q.subquery(name=name)

def measurement_attached_to_condition_episode(
    *,
    concept_ids: Iterable[int] | None = None,
    include_cols: Sequence[SQLExpr] = (),
    name: str,
    unlinked_only: bool = True,
) -> sa.Subquery:
    core = measurement_event_core(
        concept_ids=concept_ids,
        include_cols=include_cols,
        name=f"{name}_core",
        unlinked_only=unlinked_only,
    )

    return attach_to_condition_episode(
        core,
        event_id_col=core.c.event_id,
        date_col=core.c.event_date,
        person_col=core.c.person_id,
        name=name,
    )

def episode_relevant_window(
    starting_query: sa.Subquery,
    *,
    max_days_post: int = 90,
    max_days_prior: int = 30,
    name: str | None = None,
) -> sa.Subquery:
    return (
        sa.select(
            sa.func.row_number().over().label("mv_id"),
            *starting_query.c,
        )
        .where(
            sa.and_(
                starting_query.c.episode_delta_days <= max_days_post,
                starting_query.c.episode_delta_days >= -1 * max_days_prior,
            )
        )
        .subquery(name=name or starting_query.name)
    )