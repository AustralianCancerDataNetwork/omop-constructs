import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.sql import ColumnElement
from typing import Iterable, Any
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


def get_episode_hierarchy_query(
    parent_episode_subq: sa.Subquery,
    child_episode_subq: sa.Subquery,
    name: str = "episode_optional_children",
    parent_label: str = 'disease',
    child_label: str = 'extent'
) -> sa.Subquery:
    """
    Expands an episode subquery to optionally include child episodes.
    """

    return (
        sa.select(
            parent_episode_subq.c.episode_id.label(f"{parent_label}_episode_id"),
            parent_episode_subq.c.person_id.label("person_id"),
            parent_episode_subq.c.episode_concept_id.label(f"{parent_label}_episode_concept_id"),
            parent_episode_subq.c.episode_label.label(f"{parent_label}_episode_label"),
            parent_episode_subq.c.episode_start_date.label(f"{parent_label}_episode_start_date"),
            parent_episode_subq.c.episode_end_date.label(f"{parent_label}_episode_end_date"),

            child_episode_subq.c.episode_id.label(f"{child_label}_episode_id"),
            child_episode_subq.c.episode_concept_id.label(f"{child_label}_episode_concept_id"),
            child_episode_subq.c.episode_label.label(f"{child_label}_episode_label"),
            child_episode_subq.c.episode_start_date.label(f"{child_label}_start_date"),
            child_episode_subq.c.episode_end_date.label(f"{child_label}_end_date"),
        )
        .join(
            child_episode_subq,
            child_episode_subq.c.episode_parent_id == parent_episode_subq.c.episode_id,
            isouter=True,  
        )
        .subquery(name=name)        
    )