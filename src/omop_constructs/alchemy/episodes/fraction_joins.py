import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Episode, Episode_Event, Concept
from omop_semantics.runtime.default_valuesets import runtime
from ..modifiers import ModifiedProcedure
from ...semantics import registry

rt_proc_concept = so.aliased(Concept, name='rt_proc_concept')
fraction_concept = so.aliased(Concept, name='fraction_concept')

fraction_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        ModifiedProcedure.person_id,
        ModifiedProcedure.procedure_datetime,
        ModifiedProcedure.procedure_occurrence_id,
        ModifiedProcedure.procedure_concept_id,
        ModifiedProcedure.procedure_concept,
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime,
        Episode.episode_id.label('fraction_id'),
        Episode.episode_number.label('fraction_number'),
        Episode.episode_parent_id.label('course_id'),
        fraction_concept.concept_name.label('fraction_name')
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==ModifiedProcedure.procedure_occurrence_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.procedure_occurrence_id
        )
    )
    .join(Episode, Episode.episode_id==Episode_Event.episode_id)
    .join(fraction_concept, fraction_concept.concept_id==Episode.episode_object_concept_id)
    .filter(ModifiedProcedure.procedure_concept_id.in_(list(registry['rt_procedures'].all_concepts)))
)