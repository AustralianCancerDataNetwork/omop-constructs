from omop_alchemy.cdm.model import Measurement
from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from .event_factories import measurement_attached_to_condition_episode, episode_relevant_window

weight_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.weight], # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="weight",
)

weight_change_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.weight_change],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="weight_change",
)

height_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.height],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="height",
)

bsa_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.bsa],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="bsa",
)

creatinine_clearance_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.creatinine_clearance],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="creatinine_clearance",
)

est_gfr_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.egfr],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="egfr",
)

fev1_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.fev1],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="fev1",
)

distress_thermometer_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.proms_numeric.distress_thermometer],  # type: ignore
    include_cols=[Measurement.value_as_number],
    name="dtherm",
)

ecog_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.performance_status_measurements.ecog_performance_status],  # type: ignore
    include_cols=[Measurement.value_as_number],
    name="ecog",
)

smoking_pyh_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.smoking_numeric.pyh],  # type: ignore
    include_cols=[Measurement.value_as_number],
    name="smoking_pyh",
)


weight_dx = episode_relevant_window(weight_query, max_days_post=90, max_days_prior=30, name="weight_dx")
weight_change_dx = episode_relevant_window(weight_change_query, max_days_post=90, max_days_prior=30, name="weight_change_dx")
height_dx = episode_relevant_window(height_query, max_days_post=90, max_days_prior=30, name="height_dx")
bsa_dx = episode_relevant_window(bsa_query, max_days_post=90, max_days_prior=30, name="bsa_dx")
creat_dx = episode_relevant_window(creatinine_clearance_query, max_days_post=90, max_days_prior=30, name="creat_dx")
egfr_dx = episode_relevant_window(est_gfr_query, max_days_post=90, max_days_prior=30, name="egfr_dx")
fev1_dx = episode_relevant_window(fev1_query, max_days_post=90, max_days_prior=30, name="fev1_dx")
dtherm_dx = episode_relevant_window(distress_thermometer_query, max_days_post=90, max_days_prior=30, name="dtherm_dx")
ecog_dx = episode_relevant_window(ecog_query, max_days_post=90, max_days_prior=30, name="ecog_dx")
pyh_dx = episode_relevant_window(smoking_pyh_query, max_days_post=90, max_days_prior=30, name="pyh_dx")