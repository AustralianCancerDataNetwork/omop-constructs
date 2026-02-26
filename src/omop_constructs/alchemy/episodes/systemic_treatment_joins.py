import sqlalchemy as sa
from omop_alchemy.cdm.model import Episode, Episode_Event
from omop_semantics.runtime.default_valuesets import runtime
from ..modifiers import ModifiedProcedure
from .cycle_mv import CycleMV


cycle_summary_join = (
    sa.select(
        CycleMV.person_id,
        CycleMV.cycle_id,
        CycleMV.cycle_number,
        CycleMV.regimen_id,
        CycleMV.cycle_concept,
        sa.func.min(CycleMV.drug_exposure_start_date).label('first_exposure_date'),
        sa.func.max(CycleMV.drug_exposure_end_date).label('last_exposure_date'),
        sa.func.count(CycleMV.drug_exposure_id).label('exposure_count'),
    )
    .group_by(CycleMV.person_id, CycleMV.cycle_id, CycleMV.cycle_number, CycleMV.regimen_id, CycleMV.cycle_concept)
    .subquery()
)

regimen_join = (
    sa.select(
        *cycle_summary_join.c,
        sa.func.row_number().over().label('mv_id'),
        Episode.episode_number.label('regimen_number'),
        Episode.episode_parent_id.label('condition_episode_id'),
        ModifiedProcedure.procedure_occurrence_id.label('regimen_prescription_id'),
        ModifiedProcedure.procedure_concept_id.label('regimen_concept_id'),
        ModifiedProcedure.procedure_concept.label('regimen_concept'),
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime,
    )
    .join(
        Episode, 
        sa.and_(
            Episode.episode_id == cycle_summary_join.c.regimen_id, 
            Episode.episode_concept_id==runtime.types.treatment_episode_types.treatment_regimen
        ),
        isouter=True
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.episode_id==Episode.episode_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.procedure_occurrence_id
        ),
        isouter=True
    )
    .join(ModifiedProcedure, ModifiedProcedure.procedure_occurrence_id==Episode_Event.event_id, isouter=True)
)
