import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.cdm.model import Concept, Episode, Provider, Visit_Occurrence
from omop_semantics.runtime.default_valuesets import runtime  # type: ignore


provider_concept = so.aliased(Concept, name="provider_concept")

episode_of_care = (
    sa.select(
        Episode.person_id,
        Episode.episode_id,
        Episode.episode_start_date,
    )
    .where(
        Episode.episode_concept_id
        == runtime.types.disease_episode_types.episode_of_care  # type: ignore[attr-defined]
    )
    .subquery(name="episode_of_care")
)

visit_start = sa.func.coalesce(
    Visit_Occurrence.visit_start_datetime,
    sa.cast(Visit_Occurrence.visit_start_date, sa.DateTime),
)
episode_start = sa.cast(episode_of_care.c.episode_start_date, sa.DateTime)

diff_days = sa.func.extract("epoch", visit_start - episode_start) / 86400.0

episode_start_prior = sa.case(
    (
        sa.func.abs(diff_days) < 180,
        1,
    ),
    (
        visit_start > episode_start,
        2,
    ),
    else_=3,
)

visits_by_specialty = (
    sa.select(
        Visit_Occurrence.person_id,
        Visit_Occurrence.visit_occurrence_id,
        Visit_Occurrence.visit_start_date,
        Visit_Occurrence.visit_start_datetime,
        Provider.provider_id,
        provider_concept.concept_name.label("provider_specialty"),
        provider_concept.concept_id.label("provider_specialty_concept_id"),
        episode_of_care.c.episode_id,
        episode_of_care.c.episode_start_date,
        episode_start_prior.label("episode_prior"),
        diff_days.label("diff_days"),
        sa.func.row_number()
        .over(
            partition_by=Visit_Occurrence.visit_occurrence_id,
            order_by=[episode_start_prior, diff_days],
        )
        .label("rank"),
    )
    .join(episode_of_care, episode_of_care.c.person_id == Visit_Occurrence.person_id)
    .join(Provider, Visit_Occurrence.provider_id == Provider.provider_id)
    .join(provider_concept, provider_concept.concept_id == Provider.specialty_concept_id)
    .subquery(name="visits_by_specialty")
)

dx_relevant_visits = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        *visits_by_specialty.c,
    )
    .where(
        sa.or_(
            visits_by_specialty.c.episode_prior == 1,
            visits_by_specialty.c.rank == 1,
        )
    )
    .subquery(name="dx_relevant_visits")
)
