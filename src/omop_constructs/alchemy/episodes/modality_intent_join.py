import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Episode, Episode_Event, Measurement, Drug_Exposure, Procedure_Occurrence, Concept
from omop_semantics.runtime.default_valuesets import runtime
from ...semantics import registry

intent_concept = so.aliased(Concept, name='intent_concept')

modality_sact = (
    sa.select(
        sa.literal(True).label('sact'),
        Episode_Event.episode_id
    )
    .join(
        Drug_Exposure, 
        sa.and_(
            Drug_Exposure.drug_exposure_id==Episode_Event.event_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.drug_exposure_id
        )
    )
    .distinct(Episode_Event.episode_id)
    .subquery()
)
modality_rt = (
    sa.select(
        sa.literal(True).label('rt'),
        Episode_Event.episode_id
    )
    .join(
        Procedure_Occurrence, 
        sa.and_(
            Procedure_Occurrence.procedure_occurrence_id==Episode_Event.event_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.procedure_occurrence_id
        )
    )
    .filter(
        Procedure_Occurrence.procedure_concept_id.in_(list(registry['rt_procedures'].all_concepts))
    )
    .distinct(Episode_Event.episode_id)
    .subquery()
)
episode_intent = (
    sa.select(
        Episode.episode_id,
        Episode.episode_start_date,
        Episode.episode_end_date,
        Measurement.measurement_concept_id,
        intent_concept.concept_name,
    )
    .join(
        Measurement,
        sa.and_(
            Measurement.measurement_event_id==Episode.episode_id,
            Measurement.meas_event_field_concept_id==runtime.modifiers.modifier_fields.episode_id
        )
    )
    .join(intent_concept, intent_concept.concept_id==Measurement.measurement_concept_id)
    .subquery()
)

episode_join = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        episode_intent.c.episode_id,
        episode_intent.c.episode_start_date,
        episode_intent.c.episode_end_date,
        episode_intent.c.measurement_concept_id,
        episode_intent.c.concept_name,
        modality_rt.c.rt,
        modality_sact.c.sact,
        sa.and_(
            sa.func.coalesce(modality_rt.c.rt, False),
            sa.func.coalesce(modality_sact.c.sact, False)
        ).label("concurrent")
    )
    .join(modality_rt, modality_rt.c.episode_id==episode_intent.c.episode_id, isouter=True)
    .join(modality_sact, modality_sact.c.episode_id==episode_intent.c.episode_id, isouter=True)
)