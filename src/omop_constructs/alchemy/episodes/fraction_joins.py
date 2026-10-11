import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Episode, Concept
from omop_semantics.runtime.default_valuesets import runtime
from .treatment_inputs import guarded_procedures
from ...semantics import registry

rt_proc_concept = so.aliased(Concept, name='rt_proc_concept')
fraction_concept = so.aliased(Concept, name='fraction_concept')
course = so.aliased(Episode, name="fraction_course")
owner = so.aliased(Episode, name="fraction_diagnosis")

fraction_join = (
    sa.select(
        guarded_procedures.c.person_id,
        guarded_procedures.c.procedure_datetime,
        guarded_procedures.c.procedure_occurrence_id,
        guarded_procedures.c.procedure_concept_id,
        guarded_procedures.c.procedure_concept,
        guarded_procedures.c.intent_concept,
        guarded_procedures.c.intent_concept_id,
        guarded_procedures.c.intent_datetime,
        guarded_procedures.c.intent_id,
        Episode.episode_id.label('fraction_id'),
        Episode.episode_number.label('fraction_number'),
        Episode.episode_parent_id.label('course_id'),
        fraction_concept.concept_name.label('fraction_name')
    )
    .join(Episode, sa.and_(Episode.episode_id == guarded_procedures.c.episode_id,
                          Episode.person_id == guarded_procedures.c.person_id,
                          Episode.episode_concept_id == runtime.types.treatment_episode_types.treatment_cycle))
    .join(course, sa.and_(course.episode_id == Episode.episode_parent_id,
                         course.person_id == Episode.person_id,
                         course.episode_concept_id == runtime.types.treatment_episode_types.treatment_regimen))
    .join(owner, sa.and_(owner.episode_id == course.episode_parent_id,
                        owner.person_id == course.person_id,
                        owner.episode_concept_id.in_(runtime.types.disease_episode_types.ids)))
    .join(fraction_concept, fraction_concept.concept_id==Episode.episode_object_concept_id)
    .filter(guarded_procedures.c.procedure_concept_id.in_(list(registry['rt_procedures'].all_concepts)))
    .distinct()
    .subquery("distinct_fractions")
)

fraction_join = sa.select(sa.func.row_number().over().label("mv_id"), *fraction_join.c)
