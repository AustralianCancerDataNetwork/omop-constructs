import sqlalchemy as sa
import sqlalchemy.orm as so
from .systemic_treatment_mv import SACTRegimenMV
from .course_mv import RTCourseMV
from .cycle_mv import CycleMV
from .fraction_mv import FractionMV
from omop_alchemy.cdm.model import Episode_Event, Condition_Occurrence, Concept
from omop_semantics.runtime.default_valuesets import runtime

condition_concept = so.aliased(Concept, name='condition_concept')

# Count physical sources before prescription/intent detail creates repeated rows.
_regimen_sources = (
    sa.select(CycleMV.person_id, CycleMV.regimen_id,
              sa.func.count(sa.distinct(CycleMV.drug_exposure_id)).label("exposure_count"))
    .group_by(CycleMV.person_id, CycleMV.regimen_id).subquery("regimen_sources")
)
_course_sources = (
    sa.select(FractionMV.person_id, FractionMV.course_id,
              sa.func.count(sa.distinct(FractionMV.procedure_occurrence_id)).label("fraction_count"))
    .group_by(FractionMV.person_id, FractionMV.course_id).subquery("course_sources")
)

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
        sa.func.max(_regimen_sources.c.exposure_count).label('exposure_count'),
        sa.func.count(sa.distinct(SACTRegimenMV.regimen_id)).label('regimen_count'),
    )
    .join(_regimen_sources, sa.and_(_regimen_sources.c.regimen_id == SACTRegimenMV.regimen_id,
                                    _regimen_sources.c.person_id == SACTRegimenMV.person_id))
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
        sa.func.max(_course_sources.c.fraction_count).label('fraction_count'),
        sa.func.count(sa.distinct(RTCourseMV.course_id)).label('course_count'),
    )
    .join(_course_sources, sa.and_(_course_sources.c.course_id == RTCourseMV.course_id,
                                   _course_sources.c.person_id == RTCourseMV.person_id))
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
    .join(regimen_summary_join, sa.and_(regimen_summary_join.c.condition_episode_id == Episode_Event.episode_id, regimen_summary_join.c.person_id == Condition_Occurrence.person_id), isouter=True)
    .join(course_summary_join, sa.and_(course_summary_join.c.condition_episode_id == Episode_Event.episode_id, course_summary_join.c.person_id == Condition_Occurrence.person_id), isouter=True)
    .subquery()
)