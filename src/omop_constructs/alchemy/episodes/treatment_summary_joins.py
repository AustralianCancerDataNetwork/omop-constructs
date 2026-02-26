import sqlalchemy as sa
import sqlalchemy.orm as so
from .systemic_treatment_mv import SACTRegimenMV
from .course_mv import RTCourseMV
from omop_alchemy.cdm.model import Episode_Event, Condition_Occurrence, Concept
from omop_semantics.runtime.default_valuesets import runtime

condition_concept = so.aliased(Concept, name='condition_concept')

regimen_summary_join = (
    sa.select(
        SACTRegimenMV.person_id,
        SACTRegimenMV.regimen_concept,
        SACTRegimenMV.regimen_id,
        SACTRegimenMV.regimen_number,
        SACTRegimenMV.condition_episode_id,
        SACTRegimenMV.intent_concept_id.label('sact_intent_concept_id'),
        SACTRegimenMV.intent_concept.label('sact_intent_concept'),
        sa.func.min(SACTRegimenMV.first_exposure_date).label('regimen_start_date'),
        sa.func.max(SACTRegimenMV.last_exposure_date).label('regimen_end_date'),
        sa.func.sum(SACTRegimenMV.exposure_count).label('exposure_count'),
        sa.func.count(SACTRegimenMV.regimen_id).label('regimen_count'),
    )
    .group_by(SACTRegimenMV.person_id, SACTRegimenMV.regimen_concept, SACTRegimenMV.regimen_id, SACTRegimenMV.regimen_number, SACTRegimenMV.condition_episode_id, SACTRegimenMV.intent_concept_id, SACTRegimenMV.intent_concept)
    .subquery()
)

course_summary_join = (
    sa.select(
        RTCourseMV.person_id,
        RTCourseMV.course_concept,
        RTCourseMV.course_id,
        RTCourseMV.course_number,
        RTCourseMV.condition_episode_id,
        RTCourseMV.intent_concept_id.label('rt_intent_concept_id'),
        RTCourseMV.intent_concept.label('rt_intent_concept'),
        sa.func.min(RTCourseMV.first_exposure_date).label('course_start_date'),
        sa.func.max(RTCourseMV.last_exposure_date).label('course_end_date'),
        sa.func.sum(RTCourseMV.fraction_count).label('fraction_count'),
        sa.func.count(RTCourseMV.course_id).label('course_count'),
    )
    .group_by(RTCourseMV.person_id, RTCourseMV.course_concept, RTCourseMV.course_id, RTCourseMV.course_number, RTCourseMV.condition_episode_id, RTCourseMV.intent_concept_id, RTCourseMV.intent_concept)
    .subquery()
)

condition_treatment_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_start_date,
        Condition_Occurrence.condition_end_date,
        Episode_Event.episode_id.label('condition_episode_id'),
        *regimen_summary_join.c,
        *course_summary_join.c,
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Condition_Occurrence.condition_occurrence_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.condition_occurrence_id
        ),
        isouter=True
    )
    .join(regimen_summary_join, regimen_summary_join.c.condition_episode_id==Episode_Event.episode_id, isouter=True)
    .join(course_summary_join, course_summary_join.c.condition_episode_id==Episode_Event.episode_id, isouter=True)
    .subquery()
)