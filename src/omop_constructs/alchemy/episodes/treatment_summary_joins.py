import sqlalchemy as sa
from omop_semantics.runtime.default_valuesets import runtime
from .condition_episode_mv import TreatmentRegimenCycleMV

treatment_summary = (
    sa.select(
        TreatmentRegimenCycleMV.person_id,
        TreatmentRegimenCycleMV.episode_parent_id.label("condition_episode_id"),
        TreatmentRegimenCycleMV.episode_concept_id,
        TreatmentRegimenCycleMV.episode_label,

        sa.func.min(TreatmentRegimenCycleMV.episode_start_date).label("treatment_start_date"),
        sa.func.max(TreatmentRegimenCycleMV.episode_end_date).label("treatment_end_date"),

        sa.func.count(sa.distinct(TreatmentRegimenCycleMV.episode_id)).label("regimen_count"),
        sa.func.count(sa.distinct(TreatmentRegimenCycleMV.cycle_episode_id)).label("cycle_count"),
    )
    .group_by(
        TreatmentRegimenCycleMV.person_id,
        TreatmentRegimenCycleMV.episode_parent_id,
        TreatmentRegimenCycleMV.episode_concept_id,
        TreatmentRegimenCycleMV.episode_label,
    )
    .subquery("treatment_summary")
)