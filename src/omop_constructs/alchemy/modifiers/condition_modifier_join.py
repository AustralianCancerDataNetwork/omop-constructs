
from omop_alchemy.cdm.model import Condition_Occurrence, Concept, Episode
from omop_alchemy.toolkit.core.events import canonical_event_projection
from omop_alchemy.toolkit.episodes.derivation import (
    EpisodeAttachmentPolicy,
    episode_attachment_queries,
)
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
    MetastaticDiseaseModifierMV,
    AllStageModifierMV
)

condition_concept = so.aliased(Concept, name='condition_concept')
stage_concept = so.aliased(Concept, name='stage_concept')


def _condition_modifier_predicate(modifier):
    """Match modifier evidence to a condition's field, event, and person."""
    return sa.and_(
        modifier.meas_event_field_concept_id
        == runtime.modifiers.modifier_fields.condition_occurrence_id,
        Condition_Occurrence.condition_occurrence_id
        == modifier.measurement_event_id,
        Condition_Occurrence.person_id == modifier.person_id,
    )


# Both public condition views retain Condition_Occurrence as their null-bearing
# spine, but accept episodes only through Alchemy's validated explicit path.
# Using the CDM Episode source keeps valid links to any episode type visible;
# downstream constructs decide whether a linked episode is in their scope.
_modified_condition_events = canonical_event_projection(
    Condition_Occurrence
).subquery("modified_condition_events")
_condition_attachment_queries = episode_attachment_queries(
    _modified_condition_events,
    policy=EpisodeAttachmentPolicy.explicit_only,
    episodes=Episode,
    include_diagnostics=True,
)
_condition_attachments = _condition_attachment_queries.attachments.subquery(
    "condition_attachments"
)
# Kept beside the public selectables so validation tooling can distinguish a
# rejected explicit link from an event that had no Episode_Event assertion.
condition_attachment_diagnostics = _condition_attachment_queries.diagnostics

modified_conditions_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        condition_concept.concept_code.label('condition_code'),

        _condition_attachments.c.episode_id.label('condition_episode'),
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
    	GradeModifierMV.measurement_concept_id.label('grade_concept_id'),
    	GradeModifierMV.concept_name.label('grade_label'),
    	SizeModifierMV.measurement_id.label('size_id'),
    	SizeModifierMV.measurement_date.label('size_date'),
    	SizeModifierMV.measurement_concept_id.label('size_concept_id'),
    	SizeModifierMV.value_as_number.label('size_value'),
    	SizeModifierMV.unit_concept_id.label('size_unit_concept_id'),
    	SizeModifierMV.concept_name.label('size_label'),
    	LateralityModifierMV.measurement_id.label('laterality_id'),
    	LateralityModifierMV.measurement_date.label('laterality_date'),
    	LateralityModifierMV.value_as_concept_id.label('laterality_concept_id'),
    	LateralityModifierMV.concept_name.label('laterality_label'),
    	MetastaticDiseaseModifierMV.measurement_id.label('metastatic_disease_id'),
    	MetastaticDiseaseModifierMV.measurement_date.label('metastatic_disease_date'),
    	MetastaticDiseaseModifierMV.measurement_concept_id.label('metastatic_disease_concept_id'),
    	MetastaticDiseaseModifierMV.concept_name.label('metastatic_disease_label'),
    )
    .join(
        _condition_attachments,
        sa.and_(
            _condition_attachments.c.event_id
            == Condition_Occurrence.condition_occurrence_id,
            _condition_attachments.c.person_id
            == Condition_Occurrence.person_id,
        ),
        isouter=True
    )
    .join(
        TStageMV, 
        _condition_modifier_predicate(TStageMV),
        isouter=True
    )
    .join(
        NStageMV, 
        _condition_modifier_predicate(NStageMV),
        isouter=True
    )
    .join(
        MStageMV, 
        _condition_modifier_predicate(MStageMV),
        isouter=True
    )
    .join(
        GroupStageMV, 
        _condition_modifier_predicate(GroupStageMV),
        isouter=True
    )
    .join(
        GradeModifierMV, 
        _condition_modifier_predicate(GradeModifierMV),
        isouter=True
    )
    .join(
        SizeModifierMV, 
        _condition_modifier_predicate(SizeModifierMV),
        isouter=True
    )
    .join(
        LateralityModifierMV, 
        _condition_modifier_predicate(LateralityModifierMV),
        isouter=True
    )
    .join(
        MetastaticDiseaseModifierMV, 
        _condition_modifier_predicate(MetastaticDiseaseModifierMV),
        isouter=True
    )
    # Intentional inner lookup: Condition_Occurrence is this construct's required
    # spine, and a condition absent from the deployed vocabulary is not a
    # reportable condition row.  This is not a join through nullable modifier
    # evidence.
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
)

all_stage_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_start_date, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_source_value,
        Condition_Occurrence.condition_concept_id,
        condition_concept.concept_name.label('condition_concept'),
        condition_concept.concept_code.label('condition_code'),
        _condition_attachments.c.episode_id.label('condition_episode'),
    	AllStageModifierMV.measurement_id.label('stage_id'),
    	AllStageModifierMV.measurement_date.label('stage_date'),
    	AllStageModifierMV.measurement_concept_id.label('stage_concept_id'),
    	stage_concept.concept_name.label('stage_label'),
    )
    .join(
        _condition_attachments,
        sa.and_(
            _condition_attachments.c.event_id
            == Condition_Occurrence.condition_occurrence_id,
            _condition_attachments.c.person_id == Condition_Occurrence.person_id,
        ),
        isouter=True
    )
    .join(
        AllStageModifierMV, 
        _condition_modifier_predicate(AllStageModifierMV),
        isouter=True
    )
    # Intentional inner lookup: Condition_Occurrence is the required spine.
    # See the same decision recorded for modified_conditions_join above.
    .join(condition_concept, condition_concept.concept_id==Condition_Occurrence.condition_concept_id)
    # A stage is optional evidence.  Keep its label lookup outer so an
    # unstaged condition retains its null-bearing StageModifier spine row.
    .join(
        stage_concept,
        stage_concept.concept_id==AllStageModifierMV.measurement_concept_id,
        isouter=True,
    )
)
