
from omop_alchemy.cdm.model import Condition_Occurrence, Episode_Event, Concept
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_semantics.runtime.default_valuesets import runtime

from .modifier_mappers import (
    TStage,
    NStage,
    MStage,
    GroupStage,
    GradeModifier,
    SizeModifier,
    LatModifier,
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
    	TStage.stage_id.label('t_stage_id'),
    	TStage.stage_date.label('t_stage_date'),
    	TStage.stage_concept_id.label('t_stage_concept_id'),
    	TStage.stage_label.label('t_stage_label'),
    	NStage.stage_id.label('n_stage_id'),
    	NStage.stage_date.label('n_stage_date'),
    	NStage.stage_concept_id.label('n_stage_concept_id'),
    	NStage.stage_label.label('n_stage_label'),
    	MStage.stage_id.label('m_stage_id'),
    	MStage.stage_date.label('m_stage_date'),
    	MStage.stage_concept_id.label('m_stage_concept_id'),
    	MStage.stage_label.label('m_stage_label'),
    	GroupStage.stage_id.label('group_stage_id'),
    	GroupStage.stage_date.label('group_stage_date'),
    	GroupStage.stage_concept_id.label('group_stage_concept_id'),
    	GroupStage.stage_label.label('group_stage_label'),
        GradeModifier.measurement_id.label('grade_id'),
    	GradeModifier.measurement_date.label('grade_date'),
    	GradeModifier.concept_name.label('grade_concept'),
    	SizeModifier.measurement_id.label('size_id'),
    	SizeModifier.measurement_date.label('size_date'),
    	SizeModifier.value_as_number.label('size_value'),
    	SizeModifier.concept_name.label('size_concept'),
    	LatModifier.measurement_id.label('laterality_id'),
    	LatModifier.measurement_date.label('laterality_date'),
    	LatModifier.concept_name.label('laterality_concept'),
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
        TStage, 
        sa.and_(
            TStage.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==TStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        NStage, 
        sa.and_(
            NStage.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==NStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        MStage, 
        sa.and_(
            MStage.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==MStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        GroupStage, 
        sa.and_(
            GroupStage.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==GroupStage.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        GradeModifier, 
        sa.and_(
            GradeModifier.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==GradeModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        SizeModifier, 
        sa.and_(
            SizeModifier.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==SizeModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(
        LatModifier, 
        sa.and_(
            LatModifier.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id,
            Condition_Occurrence.condition_occurrence_id==LatModifier.modifier_of_event_id
        ),
        isouter=True
    )
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
)