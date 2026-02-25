import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional
from omop_alchemy.cdm.model import Concept, Concept_Ancestor, Procedure_Occurrence, Observation
from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from .condition_episode_mv import ConditionEpisodeMV

rth_ca = so.aliased(Concept_Ancestor, name="rth_ca")
srg_ca = so.aliased(Concept_Ancestor, name="srg_ca")

surgical_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(srg_ca, Concept.concept_id == srg_ca.descendant_concept_id)
    .where(srg_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.surgical_procedure)
)

radiotherapy_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .where(rth_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.rt_procedure)
)

radioisotope_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .where(rth_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.rn_procedure)
)

surg_only = surgical_concepts.except_all(
    radiotherapy_concepts.union_all(radioisotope_concepts)
).subquery(name="surg_only")

radioisotopes_only = radioisotope_concepts.subquery(name="radioisotopes_only")

surg_obs_concept = so.aliased(Concept, name="surg_obs_concept")

surgical_procedure_events = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id.label("surgery_occurrence_id"),
        Procedure_Occurrence.procedure_concept_id.label("surgery_concept_id"),
        Procedure_Occurrence.procedure_datetime.label("surgery_datetime"),
        surg_only.c.concept_name.label("surgery_name"),
        surg_only.c.concept_code.label("surgery_concept_code"),
        sa.literal("procedure").label("surgery_source"),
    )
    .join(surg_only, surg_only.c.concept_id == Procedure_Occurrence.procedure_concept_id)
)

historical_surgical_events = (
    sa.select(
        Observation.person_id,
        Observation.observation_id.label("surgery_occurrence_id"),
        Observation.value_as_concept_id.label("surgery_concept_id"),
        Observation.observation_datetime.label("surgery_datetime"),
        surg_obs_concept.concept_name.label("surgery_name"),
        surg_obs_concept.concept_code.label("surgery_concept_code"),
        sa.literal("observation").label("surgery_source"),
    )
    .join(surg_obs_concept, surg_obs_concept.concept_id == Observation.value_as_concept_id)
    .where(Observation.observation_concept_id == runtime.cancer_procedures.cancer_procedure_types.historical_procedure)
)

all_cancer_relevant_surg = (
    sa.union_all(surgical_procedure_events, historical_surgical_events)
    .subquery(name="all_cancer_relevant_surg")
)

cancer_relevant_surg_select = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        ConditionEpisodeMV.person_id,
        ConditionEpisodeMV.episode_id.label("condition_episode_id"),
        ConditionEpisodeMV.episode_start_date.label("condition_start_date"),

        all_cancer_relevant_surg.c.surgery_occurrence_id,
        all_cancer_relevant_surg.c.surgery_concept_id,
        all_cancer_relevant_surg.c.surgery_datetime,
        all_cancer_relevant_surg.c.surgery_concept_code,
        all_cancer_relevant_surg.c.surgery_name,
        all_cancer_relevant_surg.c.surgery_source,
    )
    # still person-level until windowing is added
    .join(
        all_cancer_relevant_surg,
        all_cancer_relevant_surg.c.person_id == ConditionEpisodeMV.person_id,
        isouter=True,
    )
    .subquery(name="cancer_relevant_surg")
)

radioisotope_select = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        ConditionEpisodeMV.person_id,
        ConditionEpisodeMV.episode_id.label("condition_episode_id"),
        ConditionEpisodeMV.episode_start_date.label("condition_start_date"),

        Procedure_Occurrence.procedure_occurrence_id.label("ri_occurrence_id"),
        Procedure_Occurrence.procedure_concept_id.label("ri_concept_id"),
        Procedure_Occurrence.procedure_datetime.label("ri_datetime"),
        radioisotopes_only.c.concept_name.label("ri_name"),
        radioisotopes_only.c.concept_code.label("ri_concept_code"),
        sa.literal("radioisotope_procedure").label("ri_source"),
    )
    .join(
        Procedure_Occurrence,
        Procedure_Occurrence.person_id == ConditionEpisodeMV.person_id,
        isouter=True,
    )
    .join(
        radioisotopes_only,
        radioisotopes_only.c.concept_id == Procedure_Occurrence.procedure_concept_id,
        isouter=True,
    )
    .subquery(name="radioisotope_procedure")
)

