import sqlalchemy as sa
from omop_alchemy.cdm.model import Death
from ..modifiers.condition_modifier_mv import ModifiedCondition
from .surgical_procedure_mv import SurgicalProcedureMV
from .systemic_treatment_mv import SACTRegimenMV
from .course_mv import RTCourseMV


# ---------------------------------------------------------------------------
# Modality windows — one subquery per treatment type, keyed on condition_episode_id
# ---------------------------------------------------------------------------
# All three subqueries expose a first_ and last_ date for their modality so
# that both earliest_treatment and latest_treatment can be computed consistently
# across modalities. They are outer-joined into treatment_window, so episodes
# with no activity for a given modality contribute nulls rather than being
# excluded.

# Surgery window.
# surgery_datetime is a DateTime (timestamp) in SurgicalProcedureMV, while
# SACT and RT exposure dates are Date. Casting here to Date ensures all three
# windows operate in the same unit (calendar day) before being combined in
# LEAST/GREATEST, preventing implicit timestamp-at-midnight promotion from
# producing fractional-day results in downstream scalars.
surg_window = (
    sa.select(
        SurgicalProcedureMV.condition_episode_id,
        sa.func.min(sa.cast(SurgicalProcedureMV.surgery_datetime, sa.Date)).label('first_surgery'),
        sa.func.max(sa.cast(SurgicalProcedureMV.surgery_datetime, sa.Date)).label('last_surgery'),
    )
    .group_by(SurgicalProcedureMV.condition_episode_id)
    .subquery(name='surg_window')
)

# SACT window.
# first_exposure_date / last_exposure_date are mapped as Date on SACTRegimenMV, but
# the underlying PostgreSQL columns can be timestamp without time zone depending on
# how the materialized view was originally created. Explicit casts here ensure the
# aggregated values are Date regardless of what PostgreSQL infers from the source.
# Without this, GREATEST(date, timestamp, timestamp) silently upcasts to timestamp
# and downstream date arithmetic produces interval instead of integer days.
sact_window = (
    sa.select(
        SACTRegimenMV.condition_episode_id,
        sa.func.min(sa.cast(SACTRegimenMV.first_exposure_date, sa.Date)).label('first_sact_exposure'),
        sa.func.max(sa.cast(SACTRegimenMV.last_exposure_date, sa.Date)).label('last_sact_exposure'),
    )
    .group_by(SACTRegimenMV.condition_episode_id)
    .subquery(name='sact_window')
)

# RT window.
# Same timestamp-guard cast as sact_window above — see comment there.
rt_window = (
    sa.select(
        RTCourseMV.condition_episode_id,
        sa.func.min(sa.cast(RTCourseMV.first_exposure_date, sa.Date)).label('first_rt_exposure'),
        sa.func.max(sa.cast(RTCourseMV.last_exposure_date, sa.Date)).label('last_rt_exposure'),
    )
    .group_by(RTCourseMV.condition_episode_id)
    .subquery(name='rt_window')
)


# ---------------------------------------------------------------------------
# treatment_window — episode-level summary of all modality windows
# ---------------------------------------------------------------------------
treatment_window = (
    sa.select(
        ModifiedCondition.person_id,
        ModifiedCondition.condition_episode,
        ModifiedCondition.condition_start_date,
        surg_window.c.first_surgery,
        surg_window.c.last_surgery,
        sact_window.c.first_sact_exposure,
        sact_window.c.last_sact_exposure,
        rt_window.c.first_rt_exposure,
        rt_window.c.last_rt_exposure,
        # concurrent_chemort: True when the start of one modality falls within
        # the treatment window of the other, indicating overlapping SACT and RT.
        # Uses >= / <= so that same-day starts — the most common chemoradiation
        # pattern — are correctly classified as concurrent. 
        # NULL when either modality is absent for the episode.
        sa.case(
            (
                (sact_window.c.first_sact_exposure.isnot(None) & rt_window.c.first_rt_exposure.isnot(None)),
                sa.or_(
                    sa.and_(
                        sact_window.c.first_sact_exposure >= rt_window.c.first_rt_exposure,
                        sact_window.c.first_sact_exposure <= rt_window.c.last_rt_exposure,
                    ),
                    sa.and_(
                        rt_window.c.first_rt_exposure >= sact_window.c.first_sact_exposure,
                        rt_window.c.first_rt_exposure <= sact_window.c.last_sact_exposure,
                    ),
                ),
            ),
            else_=None,
        ).label('concurrent_chemort'),
    )
    .select_from(ModifiedCondition)
    .join(surg_window, surg_window.c.condition_episode_id == ModifiedCondition.condition_episode, isouter=True)
    .join(rt_window, rt_window.c.condition_episode_id == ModifiedCondition.condition_episode, isouter=True)
    .join(sact_window, sact_window.c.condition_episode_id == ModifiedCondition.condition_episode, isouter=True)
    .distinct()
    .subquery()
)


# ---------------------------------------------------------------------------
# treatment_envelope — condition-episode spine joined to death
# ---------------------------------------------------------------------------
# The spine is treatment_window (rooted at ModifiedCondition / ConditionEpisodeMV)
# so that every condition episode appears regardless of mortality status.
# Death is outer-joined on person_id to attach death_datetime where it exists.
# Using Death as the spine instead would silently exclude all living patients —
# treatment timing scalars such as days_from_dx_to_treatment are meaningful for
# the full cohort, not only for those with a death record.
treatment_envelope = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        treatment_window.c.person_id,
        treatment_window.c.condition_episode,
        treatment_window.c.condition_start_date,
        treatment_window.c.concurrent_chemort,
        # earliest_treatment: first treatment event across all modalities.
        # All three inputs are Date (cast explicitly in each of surg_window,
        # sact_window, and rt_window), so LEAST returns a Date and the
        # downstream scalar is a whole-day integer.
        # Note: PostgreSQL LEAST/GREATEST ignore NULLs and only return NULL
        # when all inputs are NULL. This diverges from the SQL standard (where
        # any NULL propagates) but is the correct behaviour here — a missing
        # modality should not suppress a real date from another. No COALESCE
        # sentinel is needed; this is intentional and PostgreSQL-specific.
        sa.func.least(
            treatment_window.c.first_surgery,
            treatment_window.c.first_sact_exposure,
            treatment_window.c.first_rt_exposure,
        ).label('earliest_treatment'),
        # latest_treatment: last treatment event across all modalities.
        # Surgery (last_surgery) is included so that surgery-only episodes and
        # episodes where surgery was the final treatment produce a populated
        # value. All three inputs are Date — same cast guarantee as LEAST above.
        sa.func.greatest(
            treatment_window.c.last_surgery,
            treatment_window.c.last_sact_exposure,
            treatment_window.c.last_rt_exposure,
        ).label('latest_treatment'),
        Death.death_datetime,
    )
    .select_from(treatment_window)
    .join(Death, Death.person_id == treatment_window.c.person_id, isouter=True)
    .subquery()
)


# ---------------------------------------------------------------------------
# treatment_envelope_with_scalars — derived day-count fields
# ---------------------------------------------------------------------------
treatment_envelope_with_scalars = (
    sa.select(
        *treatment_envelope.c,
        # treatment_days_before_death: calendar days between last treatment and death.
        # death_datetime is cast to Date before subtraction so the result is
        # Date - Date → integer days, not a fractional value. 
        # Negative values (latest_treatment after death_datetime) are left visible
        # as they indicate a data quality issue to be handled downstream.
        sa.case(
            (
                (treatment_envelope.c.death_datetime.isnot(None) & treatment_envelope.c.latest_treatment.isnot(None)),
                sa.cast(treatment_envelope.c.death_datetime, sa.Date) - treatment_envelope.c.latest_treatment,
            ),
            else_=None,
        ).label('treatment_days_before_death'),
        # days_from_dx_to_treatment: calendar days from condition start to first treatment.
        # earliest_treatment is a Date (LEAST of three Date-typed inputs), and
        # condition_start_date is a Date, so this is Date - Date → integer days.
        sa.case(
            (
                (treatment_envelope.c.earliest_treatment.isnot(None) & treatment_envelope.c.condition_start_date.isnot(None)),
                treatment_envelope.c.earliest_treatment - treatment_envelope.c.condition_start_date,
            ),
            else_=None,
        ).label('days_from_dx_to_treatment'),
    )
)
