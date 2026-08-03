import sqlalchemy as sa
from omop_alchemy.cdm.model import Episode, Episode_Event, Drug_Exposure, Procedure_Occurrence
from omop_semantics.runtime.default_valuesets import runtime
from ...semantics import registry
from ..modifiers.procedure_modifier_mv import ModifiedProcedure

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
        Episode.episode_parent_id,
        ModifiedProcedure.intent_concept_id.label('measurement_concept_id'),
        ModifiedProcedure.intent_concept.label('concept_name'),
    )
    .join(
        Episode_Event,
        sa.and_(
            Episode_Event.episode_id==Episode.episode_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.procedure_occurrence_id
        )
    )
    .join(ModifiedProcedure, ModifiedProcedure.procedure_occurrence_id==Episode_Event.event_id)
    .filter(
        ModifiedProcedure.intent_concept_id.in_(runtime.treatment_modifiers.treatment_intent.ids)
    )
    .subquery()
)

episode_join = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        episode_intent.c.episode_id.label('treatment_episode_id'),
        episode_intent.c.episode_start_date.label('treatment_episode_start_date'),
        episode_intent.c.episode_end_date.label('treatment_episode_end_date'),
        episode_intent.c.episode_parent_id.label('treatment_episode_parent_id'),
        episode_intent.c.measurement_concept_id.label('treatment_intent_concept_id'),
        episode_intent.c.concept_name.label('treatment_intent_name'),
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