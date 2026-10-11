import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Iterable
from omop_alchemy.cdm.model import (
    Episode,
    Concept
)

from omop_semantics.runtime.default_valuesets import runtime # type: ignore

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
    child_label: str = 'extent_episode'
) -> sa.Subquery:
    """
    Expands an episode subquery to optionally include child episodes.
    """

    return (
        sa.select(
            parent_episode_subq.c.episode_id,
            parent_episode_subq.c.person_id.label("person_id"),
            parent_episode_subq.c.episode_concept_id,
            parent_episode_subq.c.episode_label,
            parent_episode_subq.c.episode_start_date,
            parent_episode_subq.c.episode_end_date,
            parent_episode_subq.c.episode_parent_id,

            child_episode_subq.c.episode_id.label(f"{child_label}_id"),
            child_episode_subq.c.episode_concept_id.label(f"{child_label}_concept_id"),
            child_episode_subq.c.episode_label.label(f"{child_label}_label"),
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

def diagnosis_attribution(episodes: sa.Subquery, roots: sa.Subquery) -> sa.Subquery:
    """Distinct self and recorded, same-person child-to-root treatment targets."""
    child = so.aliased(Episode, name="attribution_child")
    own = sa.select(
        episodes.c.person_id,
        episodes.c.episode_id.label("owner_id"),
        episodes.c.episode_id.label("target_id"),
    )
    inherited = (
        sa.select(roots.c.person_id, child.episode_id, roots.c.episode_id)
        .join(child, sa.and_(
            child.episode_id == roots.c.extent_episode_id,
            child.person_id == roots.c.person_id,
            child.episode_parent_id == roots.c.episode_id,
        ))
        .where(child.episode_concept_id.in_([
            runtime.types.disease_episode_types.disease_progression,
            runtime.types.disease_episode_types.metastatic,
        ]))
    )
    return own.union(inherited).subquery("diagnosis_attribution")


def dx_treatment_window(attribution: sa.Subquery, name="dx_treat_window"):
    Diagnosis = Episode
    Regimen = so.aliased(Episode, name="regimen")

    return (
        sa.select(
            Diagnosis.episode_id.label("dx_episode_id"),
            Diagnosis.person_id,
            Diagnosis.episode_start_date.label("dx_start_date"),
            sa.func.min(Regimen.episode_start_date).label("treatment_start"),
            sa.func.max(Regimen.episode_end_date).label("treatment_end"),
            sa.func.count(sa.distinct(Regimen.episode_id)).label("treatment_regimen_count"),
        )
        .join(
            attribution,
            sa.and_(attribution.c.target_id == Diagnosis.episode_id,
                    attribution.c.person_id == Diagnosis.person_id),
        )
        .join(
            Regimen, sa.and_(
                Regimen.episode_parent_id == attribution.c.owner_id,
                Regimen.person_id == attribution.c.person_id,
            ),
        )
        .where(
            Diagnosis.episode_concept_id.in_(runtime.types.disease_episode_types.ids),
            Regimen.episode_concept_id.in_(runtime.types.treatment_episode_types.ids),
        )
        .group_by(
            Diagnosis.episode_id,
            Diagnosis.person_id,
            Diagnosis.episode_start_date,
        )
        .subquery(name)
    )
