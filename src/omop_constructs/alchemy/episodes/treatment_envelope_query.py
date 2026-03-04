import sqlalchemy as sa
from omop_alchemy.cdm.model import Death
from ..modifiers import ModifiedCondition
from .surgical_procedure_mv import SurgicalProcedureMV
from .systemic_treatment_mv import SACTRegimenMV
from .course_mv import RTCourseMV


first_surg = (
    sa.select(
        SurgicalProcedureMV.condition_episode_id, 
        sa.func.min(SurgicalProcedureMV.surgery_datetime).label('first_surgery'), 
    )
    .group_by(        
        SurgicalProcedureMV.condition_episode_id,        
    )
    .subquery(name='first_surg')
)

sact_window = (
    sa.select(
        SACTRegimenMV.condition_episode_id, 
        sa.func.min(SACTRegimenMV.first_exposure_date).label('first_sact_exposure'), 
        sa.func.max(SACTRegimenMV.last_exposure_date).label('last_sact_exposure') 
    )
    .group_by(        
        SACTRegimenMV.condition_episode_id
    )
    .subquery(name='sact_window')
)

rt_window = (
    sa.select(
        RTCourseMV.condition_episode_id, 
        sa.func.min(RTCourseMV.first_exposure_date).label('first_rt_exposure'), 
        sa.func.max(RTCourseMV.last_exposure_date).label('last_rt_exposure') 
    )
    .group_by(        
        RTCourseMV.condition_episode_id
    )
    .subquery(name='rt_window')
)



treatment_window = (
    sa.select(
        ModifiedCondition.person_id, 
        ModifiedCondition.condition_episode, 
        ModifiedCondition.condition_start_date,
        first_surg.c.first_surgery,
        sact_window.c.first_sact_exposure,
        sact_window.c.last_sact_exposure,
        rt_window.c.first_rt_exposure,
        rt_window.c.last_rt_exposure,
        sa.case(
            (
                (sact_window.c.first_sact_exposure.isnot(None)&rt_window.c.first_rt_exposure.isnot(None)),
                sa.or_(
                    sa.and_(
                        sact_window.c.first_sact_exposure > rt_window.c.first_rt_exposure,
                        sact_window.c.first_sact_exposure < rt_window.c.last_rt_exposure
                    ),
                    sa.and_(
                        rt_window.c.first_rt_exposure > sact_window.c.first_sact_exposure,
                        rt_window.c.first_rt_exposure < sact_window.c.last_sact_exposure
                    )
                )
            ),
                else_=None
        ).label('concurrent_chemort')
    )
    .join(first_surg, first_surg.c.condition_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .join(rt_window, rt_window.c.condition_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .join(sact_window, sact_window.c.condition_episode_id==ModifiedCondition.condition_episode, isouter=True)
    .distinct()
    .subquery()
)

treatment_envelope = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Death.person_id,
        treatment_window.c.condition_episode, 
        treatment_window.c.condition_start_date,
        treatment_window.c.concurrent_chemort,
        sa.func.least(treatment_window.c.first_surgery, treatment_window.c.first_sact_exposure, treatment_window.c.first_rt_exposure).label('earliest_treatment'),
        sa.func.greatest(treatment_window.c.last_sact_exposure, treatment_window.c.last_rt_exposure).label('latest_treatment'),
        Death.death_datetime
    )
    .join(treatment_window, treatment_window.c.person_id==Death.person_id, isouter=True)
    .subquery()
)

treatment_envelope_with_scalars = (
    sa.select(
        *treatment_envelope.c,
        sa.case(
            (
                (treatment_envelope.c.death_datetime.isnot(None) & treatment_envelope.c.latest_treatment.isnot(None)),
                sa.func.extract('epoch', treatment_envelope.c.death_datetime - treatment_envelope.c.latest_treatment)/86400
            ),
            else_=None
        ).label('treatment_days_before_death'),
        sa.case(
            (
                (treatment_envelope.c.earliest_treatment.isnot(None) & treatment_envelope.c.condition_start_date.isnot(None)),
                sa.func.extract('epoch', treatment_envelope.c.earliest_treatment - treatment_envelope.c.condition_start_date)/86400
            ),
            else_=None
        ).label('days_from_dx_to_treatment')
    )
)
            