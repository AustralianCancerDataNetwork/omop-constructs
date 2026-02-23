import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Iterable
from omop_alchemy.cdm.model import (
    Condition_Occurrence,
    Episode,
    Concept,
    Episode_Event
)
from omop_semantics.runtime.default_valuesets import runtime

episode_concept = so.aliased(Concept, name="episode_concept")

def get_episode_query(
    episode_concept_ids: Iterable[int],
    name: str = "episode_construct",
) -> sa.Subquery:
    """
    Base query for Episode-derived constructs.
    Guarantees at most 1 row per episode_id.
    """
    return (
        sa.select(
            Episode.episode_id,
            Episode.person_id,
            Episode.episode_start_date,
            Episode.episode_start_datetime,
            Episode.episode_end_date,
            Episode.episode_end_datetime,
            Episode.episode_concept_id,
            episode_concept.concept_name.label("episode_label"),
            Episode.episode_object_concept_id,
            Episode.episode_type_concept_id,
            Episode.episode_parent_id,
        )
        .join(
            episode_concept,
            episode_concept.concept_id == Episode.episode_concept_id,
            isouter=True,
        )
        .where(Episode.episode_concept_id.in_(list(episode_concept_ids)))
        .subquery(name=name)
    )

def require_condition_anchor(
    episode_subq: sa.Subquery,
    name: str | None = None,
) -> sa.Subquery:
    """
    Filters an Episode subquery to only episodes with at least one
    linked Condition_Occurrence via Episode_Event.
    """
    exists_condition = sa.exists().where(
        sa.and_(
            Episode_Event.episode_id == episode_subq.c.episode_id,
            Episode_Event.episode_event_field_concept_id
            == runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id == Episode_Event.event_id,
        )
    )
    return (
        sa.select(*episode_subq.c)
        .where(exists_condition)
        .subquery(name=name or episode_subq.name)
    )


def get_episode_hierarchy_query(
    parent_episode_subq: sa.Subquery,
    child_episode_subq: sa.Subquery,
    name: str = "episode_optional_children",
) -> sa.Subquery:
    """
    Expands an episode subquery to optionally include child episodes.
    """

    return (
        sa.select(
            parent_episode_subq.c.episode_id.label("parent_episode_id"),
            parent_episode_subq.c.person_id.label("person_id"),
            parent_episode_subq.c.episode_concept_id.label("parent_episode_concept_id"),
            parent_episode_subq.c.episode_label.label("parent_episode_label"),
            parent_episode_subq.c.episode_start_date.label("parent_start_date"),
            parent_episode_subq.c.episode_end_date.label("parent_end_date"),

            child_episode_subq.c.episode_id.label("child_episode_id"),
            child_episode_subq.c.episode_concept_id.label("child_episode_concept_id"),
            child_episode_subq.c.episode_label.label("child_episode_label"),
            child_episode_subq.c.episode_start_date.label("child_start_date"),
            child_episode_subq.c.episode_end_date.label("child_end_date"),
        )
        .join(
            child_episode_subq,
            child_episode_subq.c.episode_parent_id == parent_episode_subq.c.episode_id,
            isouter=True,  
        )
        .subquery(name=name)        
    )