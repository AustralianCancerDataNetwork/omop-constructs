import sqlalchemy as sa
from sqlalchemy.sql import ColumnElement

from omop_alchemy.cdm.model import Episode
from omop_semantics.runtime.default_valuesets import runtime  # type: ignore

from ..events.dx_linked_obs_mv import DxObservationMV
from ..events.dx_linked_visit_mv import DxRelevantVisitMV
from .treatment_envelope_mv import TreatmentEnvelopeMV


def days_between(
    end_date: ColumnElement,
    start_date: ColumnElement,
) -> ColumnElement:
    return sa.func.extract(
        "epoch",
        sa.cast(end_date, sa.DateTime) - sa.cast(start_date, sa.DateTime),
    ) / 86400.0


episode_of_care = (
    sa.select(
        Episode.person_id,
        Episode.episode_id,
        Episode.episode_start_date,
    )
    .where(
        Episode.episode_concept_id
        == runtime.types.disease_episode_types.episode_of_care  # type: ignore[attr-defined]
    )
    .subquery(name="episode_of_care")
)

episode_of_care_obs = (
    sa.select(*DxObservationMV.__table__.c)
    .where(
        DxObservationMV.episode_concept_id
        == runtime.types.disease_episode_types.episode_of_care  # type: ignore[attr-defined]
    )
    .subquery(name="episode_of_care_obs")
)

first_specialist = (
    sa.select(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
        sa.func.min(episode_of_care_obs.c.event_date).label("first_specialist_consult"),
        sa.func.max(episode_of_care_obs.c.event_date).label("last_specialist_consult"),
    )
    .where(
        episode_of_care_obs.c.event_concept_id.in_(
            [
                runtime.cancer_procedures.cancer_consult_types.medonc,  # type: ignore[attr-defined]
                runtime.cancer_procedures.cancer_consult_types.clinonc,  # type: ignore[attr-defined]
            ]
        )
    )
    .group_by(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
    )
    .subquery(name="first_specialist")
)

gp_referral = (
    sa.select(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
        sa.func.min(episode_of_care_obs.c.event_date).label("initial_gp_referral"),
    )
    .where(
        episode_of_care_obs.c.event_concept_id
        == runtime.cancer_procedures.cancer_consult_types.oncology_referral  # type: ignore[attr-defined]
    )
    .group_by(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
    )
    .subquery(name="gp_referral")
)

pall_care_referral = (
    sa.select(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
        sa.func.min(episode_of_care_obs.c.event_date).label("first_pall_care_referral"),
    )
    .where(
        episode_of_care_obs.c.event_concept_id
        == runtime.cancer_procedures.cancer_consult_types.pall_care_referral  # type: ignore[attr-defined]
    )
    .group_by(
        episode_of_care_obs.c.person_id,
        episode_of_care_obs.c.episode_id,
    )
    .subquery(name="pall_care_referral")
)

specialist_visit = (
    sa.select(
        DxRelevantVisitMV.person_id,
        DxRelevantVisitMV.episode_id,
        sa.func.min(DxRelevantVisitMV.visit_start_date).label("first_specialist_visit"),
    )
    .where(
        DxRelevantVisitMV.provider_specialty_concept_id.in_(
            [
                runtime.cancer_procedures.encounter_provider_specialty.radonc,  # type: ignore[attr-defined]
                runtime.cancer_procedures.encounter_provider_specialty.medonc,  # type: ignore[attr-defined]
                runtime.cancer_procedures.encounter_provider_specialty.haematologist,  # type: ignore[attr-defined]
            ]
        )
    )
    .group_by(
        DxRelevantVisitMV.person_id,
        DxRelevantVisitMV.episode_id,
    )
    .subquery(name="specialist_visit")
)

pall_care_visit = (
    sa.select(
        DxRelevantVisitMV.person_id,
        DxRelevantVisitMV.episode_id,
        sa.func.min(DxRelevantVisitMV.visit_start_date).label("first_pall_care_visit"),
    )
    .where(
        DxRelevantVisitMV.provider_specialty_concept_id
        == runtime.cancer_procedures.encounter_provider_specialty.pall_care  # type: ignore[attr-defined]
    )
    .group_by(
        DxRelevantVisitMV.person_id,
        DxRelevantVisitMV.episode_id,
    )
    .subquery(name="pall_care_visit")
)

specialist_trajectory = (
    sa.select(
        episode_of_care.c.person_id,
        episode_of_care.c.episode_id,
        episode_of_care.c.episode_start_date,
        first_specialist.c.first_specialist_consult,
        first_specialist.c.last_specialist_consult,
        specialist_visit.c.first_specialist_visit,
        gp_referral.c.initial_gp_referral,
        pall_care_referral.c.first_pall_care_referral,
        pall_care_visit.c.first_pall_care_visit,
    )
    .join(
        first_specialist,
        first_specialist.c.episode_id == episode_of_care.c.episode_id,
        isouter=True,
    )
    .join(
        gp_referral,
        gp_referral.c.episode_id == episode_of_care.c.episode_id,
        isouter=True,
    )
    .join(
        pall_care_referral,
        pall_care_referral.c.episode_id == episode_of_care.c.episode_id,
        isouter=True,
    )
    .join(
        specialist_visit,
        specialist_visit.c.episode_id == episode_of_care.c.episode_id,
        isouter=True,
    )
    .join(
        pall_care_visit,
        pall_care_visit.c.episode_id == episode_of_care.c.episode_id,
        isouter=True,
    )
    .subquery(name="specialist_trajectory")
)

consult_window = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        specialist_trajectory.c.person_id,
        specialist_trajectory.c.episode_id,
        specialist_trajectory.c.episode_start_date,
        specialist_trajectory.c.initial_gp_referral,
        sa.func.least(
            specialist_trajectory.c.first_specialist_consult,
            specialist_trajectory.c.first_specialist_visit,
        ).label("first_specialist"),
        sa.func.least(
            specialist_trajectory.c.first_pall_care_referral,
            specialist_trajectory.c.first_pall_care_visit,
        ).label("first_pall_care"),
        sa.func.least(
            specialist_trajectory.c.first_pall_care_referral,
            specialist_trajectory.c.first_pall_care_visit,
            TreatmentEnvelopeMV.earliest_treatment,
        ).label("first_pall_care_or_treatment"),
        TreatmentEnvelopeMV.earliest_treatment,
        sa.case(
            (
                sa.and_(
                    specialist_trajectory.c.initial_gp_referral.is_not(None),
                    sa.func.least(
                        specialist_trajectory.c.first_specialist_consult,
                        specialist_trajectory.c.first_specialist_visit,
                    ).is_not(None),
                ),
                days_between(
                    sa.func.least(
                        specialist_trajectory.c.first_specialist_consult,
                        specialist_trajectory.c.first_specialist_visit,
                    ),
                    specialist_trajectory.c.initial_gp_referral,
                ),
            ),
            else_=None,
        ).label("referral_to_specialist"),
        sa.case(
            (
                sa.and_(
                    specialist_trajectory.c.initial_gp_referral.is_not(None),
                    sa.func.least(
                        specialist_trajectory.c.first_pall_care_referral,
                        specialist_trajectory.c.first_pall_care_visit,
                        TreatmentEnvelopeMV.earliest_treatment,
                    ).is_not(None),
                ),
                days_between(
                    sa.func.least(
                        specialist_trajectory.c.first_pall_care_referral,
                        specialist_trajectory.c.first_pall_care_visit,
                        TreatmentEnvelopeMV.earliest_treatment,
                    ),
                    specialist_trajectory.c.initial_gp_referral,
                ),
            ),
            else_=None,
        ).label("referral_to_tx"),
    )
    .join(
        TreatmentEnvelopeMV,
        TreatmentEnvelopeMV.condition_episode == specialist_trajectory.c.episode_id,
        isouter=True,
    )
    .subquery(name="consult_window")
)
