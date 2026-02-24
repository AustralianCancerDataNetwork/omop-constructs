import sqlalchemy as sa
from omop_alchemy.cdm.model.clinical import Person, Observation, Death
from omop_alchemy.cdm.model.vocabulary import Concept
from ..episodes import ConditionEpisodeMV

from omop_semantics.runtime.default_valuesets import runtime # type: ignore

person_postcode = (
    sa.select(
        Observation.person_id,
        Observation.value_as_number.label("post_code"),
    )
    .where(Observation.observation_concept_id == runtime.observations.demography_concepts.post_code)
    .subquery()
)

person_cob = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label("country_of_birth"),
    )
    .join(Concept, Concept.concept_id == Observation.value_as_concept_id)
    .where(Observation.observation_concept_id == runtime.observations.demography_concepts.country_of_birth)
    .subquery()
)

person_lang = (
    sa.select(
        Observation.person_id,
        Concept.concept_name.label("language_spoken"),
    )
    .join(Concept, Concept.concept_id == Observation.value_as_concept_id)
    .where(Observation.observation_concept_id == runtime.observations.demography_concepts.language_spoken)
    .subquery()
)

# demographics join to condition episodes
demographics_join = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),

        Person.person_id,
        Person.year_of_birth,
        Death.death_datetime,
        Person.gender_concept_id,
        Person.person_source_value.label("mrn"),

        ConditionEpisodeMV.episode_id.label("episode_id"),
        ConditionEpisodeMV.episode_start_date.label("episode_start_date"),

        Concept.concept_name.label("sex"),
        person_lang.c.language_spoken,
        person_cob.c.country_of_birth,
        person_postcode.c.post_code,
    )
    .join(Death, Death.person_id == Person.person_id, isouter=True)
    .join(
        ConditionEpisodeMV,
        ConditionEpisodeMV.person_id == Person.person_id,
    )
    .join(
        Concept,
        Concept.concept_id == Person.gender_concept_id,
    )
    .join(person_lang, person_lang.c.person_id == Person.person_id, isouter=True)
    .join(person_cob, person_cob.c.person_id == Person.person_id, isouter=True)
    .join(person_postcode, person_postcode.c.person_id == Person.person_id, isouter=True)
)

