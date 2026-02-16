
from omop_alchemy.cdm.model import Condition_Occurrence, Episode_Event, Concept
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_semantics.runtime.default_valuesets import runtime

from .modifier_mappers import (
    TStageMV,
    NStageMV,
    MStageMV,
    GroupStageMV,
    GradeModifierMV,
    SizeModifierMV,
    LateralityModifierMV,
)

condition_concept = so.aliased(Concept, name='condition_concept')

modified_conditions_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        Episode_Event.episode_id.label('condition_episode'),
    	TStageMV.stage_id.label('t_stage_id'),
    	TStageMV.stage_date.label('t_stage_date'),
    	TStageMV.stage_concept_id.label('t_stage_concept_id'),
    	TStageMV.stage_label.label('t_stage_label'),
    	NStageMV.stage_id.label('n_stage_id'),
    	NStageMV.stage_date.label('n_stage_date'),
    	NStageMV.stage_concept_id.label('n_stage_concept_id'),
    	NStageMV.stage_label.label('n_stage_label'),
    	MStageMV.stage_id.label('m_stage_id'),
    	MStageMV.stage_date.label('m_stage_date'),
    	MStageMV.stage_concept_id.label('m_stage_concept_id'),
    	MStageMV.stage_label.label('m_stage_label'),
    	GroupStageMV.stage_id.label('group_stage_id'),
    	GroupStageMV.stage_date.label('group_stage_date'),
    	GroupStageMV.stage_concept_id.label('group_stage_concept_id'),
    	GroupStageMV.stage_label.label('group_stage_label'),
        GradeModifierMV.measurement_id.label('grade_id'),
    	GradeModifierMV.measurement_date.label('grade_date'),
    	GradeModifierMV.concept_name.label('grade_concept'),
    	SizeModifierMV.measurement_id.label('size_id'),
    	SizeModifierMV.measurement_date.label('size_date'),
    	SizeModifierMV.value_as_number.label('size_value'),
    	SizeModifierMV.concept_name.label('size_concept'),
    	LateralityModifierMV.measurement_id.label('laterality_id'),
    	LateralityModifierMV.measurement_date.label('laterality_date'),
    	LateralityModifierMV.concept_name.label('laterality_concept'),
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Condition_Occurrence.condition_occurrence_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id
        ),
        isouter=True
    )
    .join(
        TStageMV, 
        sa.and_(
            TStageMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==TStageMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        NStageMV, 
        sa.and_(
            NStageMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==NStageMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        MStageMV, 
        sa.and_(
            MStageMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==MStageMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        GroupStageMV, 
        sa.and_(
            GroupStageMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==GroupStageMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        GradeModifierMV, 
        sa.and_(
            GradeModifierMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==GradeModifierMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        SizeModifierMV, 
        sa.and_(
            SizeModifierMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==SizeModifierMV.measurement_event_id
        ),
        isouter=True
    )
    .join(
        LateralityModifierMV, 
        sa.and_(
            LateralityModifierMV.meas_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==LateralityModifierMV.measurement_event_id
        ),
        isouter=True
    )
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
)