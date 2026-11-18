"""Match individual RT courses with recorded SACT doses."""

import sqlalchemy as sa

from .course_mv import RTCourseMV
from .cycle_mv import CycleMV
from .systemic_treatment_mv import SACTRegimenMV


# The input views can repeat fractions and cycles for prescriptions and intents.
# Reduce them to course windows and attributed cycle keys before joining doses.
rt_course_windows = (
    sa.select(
        RTCourseMV.person_id,
        RTCourseMV.condition_episode_id,
        RTCourseMV.course_id,
        sa.func.min(sa.cast(RTCourseMV.first_exposure_date, sa.Date)).label("rt_start"),
        sa.func.max(sa.cast(RTCourseMV.last_exposure_date, sa.Date)).label("rt_end"),
    )
    .group_by(RTCourseMV.person_id, RTCourseMV.condition_episode_id, RTCourseMV.course_id)
    .subquery("rt_course_windows")
)

attributed_sact_cycles = (
    sa.select(
        SACTRegimenMV.person_id,
        SACTRegimenMV.condition_episode_id,
        SACTRegimenMV.regimen_id,
        SACTRegimenMV.cycle_id,
    )
    .distinct()
    .subquery("attributed_sact_cycles")
)

concurrent_treatment_pairs = (
    sa.select(
        rt_course_windows.c.person_id,
        rt_course_windows.c.condition_episode_id,
        rt_course_windows.c.course_id,
        attributed_sact_cycles.c.regimen_id,
        rt_course_windows.c.rt_start,
    )
    .select_from(rt_course_windows)
    .join(attributed_sact_cycles, sa.and_(
        attributed_sact_cycles.c.person_id == rt_course_windows.c.person_id,
        attributed_sact_cycles.c.condition_episode_id == rt_course_windows.c.condition_episode_id,
    ))
    .join(CycleMV, sa.and_(
        CycleMV.person_id == attributed_sact_cycles.c.person_id,
        CycleMV.regimen_id == attributed_sact_cycles.c.regimen_id,
        CycleMV.cycle_id == attributed_sact_cycles.c.cycle_id,
        CycleMV.drug_exposure_start_date.between(rt_course_windows.c.rt_start, rt_course_windows.c.rt_end),
    ))
    .distinct()
    .cte("concurrent_treatment_pairs")
)
