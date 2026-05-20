from ..._lazy_imports import load_export

_EXPORTS: dict[str, str] = {
    "OverarchingDiseaseEpisodeMV": "omop_constructs.alchemy.episodes.condition_episode_mv",
    "TreatmentRegimenCycleMV": "omop_constructs.alchemy.episodes.condition_episode_mv",
    "ConditionEpisodeMV": "omop_constructs.alchemy.episodes.condition_episode_mv",
    "DxTreatStartMV": "omop_constructs.alchemy.episodes.condition_episode_mv",
    "ConditionEpisodeObject": "omop_constructs.alchemy.episodes.condition_episode_objects",
    "SurgicalProcedureMV": "omop_constructs.alchemy.episodes.surgical_procedure_mv",
    "SACTRegimenMV": "omop_constructs.alchemy.episodes.systemic_treatment_mv",
    "CycleMV": "omop_constructs.alchemy.episodes.cycle_mv",
    "RTCourseMV": "omop_constructs.alchemy.episodes.course_mv",
    "FractionMV": "omop_constructs.alchemy.episodes.fraction_mv",
    "ConditionTreatmentEpisode": "omop_constructs.alchemy.episodes.treatment_summary_mv",
    "TreatmentEnvelopeMV": "omop_constructs.alchemy.episodes.treatment_envelope_mv",
    "TreatmentIntentMV": "omop_constructs.alchemy.episodes.modality_intent_mv",
    "ConditionTreatmentIntentMV": "omop_constructs.alchemy.episodes.modality_intent_mv",
    "ConsultWindowMV": "omop_constructs.alchemy.episodes.consult_window_mv",
}

__all__ = [
    "OverarchingDiseaseEpisodeMV",
    "TreatmentRegimenCycleMV",
    "ConditionEpisodeMV",
    "ConditionEpisodeObject",
    "SurgicalProcedureMV",
    "DxTreatStartMV",
    "SACTRegimenMV",
    "CycleMV",
    "RTCourseMV",
    "FractionMV",
    "ConditionTreatmentEpisode",
    "TreatmentEnvelopeMV",
    "TreatmentIntentMV",
    "ConditionTreatmentIntentMV",
    "ConsultWindowMV",
]


def __getattr__(name: str):
    return load_export(name, _EXPORTS)


def __dir__() -> list[str]:
    return sorted(__all__)
