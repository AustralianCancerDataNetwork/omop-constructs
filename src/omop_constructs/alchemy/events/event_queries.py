from omop_alchemy.cdm.model import Measurement
from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from .event_factories import (
    EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    episode_relevant_window,
    measurement_attached_to_condition_episode,
)

weight_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.weight], # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="weight",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

weight_change_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.weight_change],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="weight_change",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

height_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.height],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="height",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

bsa_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.body_size_measurements.bsa],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="bsa",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

creatinine_clearance_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.creatinine_clearance],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="creatinine_clearance",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

est_gfr_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.egfr],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="egfr",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

fev1_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.lab_measurements.fev1],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="fev1",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

distress_thermometer_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.proms_numeric.distress_thermometer],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="dtherm",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

ecog_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.performance_status_measurements.ecog_performance_status],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.value_as_concept_id, Measurement.unit_concept_id],
    name="ecog",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)

smoking_pyh_query = measurement_attached_to_condition_episode(
    concept_ids=[runtime.measurements_numeric.smoking_numeric.pyh],  # type: ignore
    include_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
    name="smoking_pyh",
    policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
)


weight_dx = episode_relevant_window(weight_query, name="weight_dx")
weight_change_dx = episode_relevant_window(weight_change_query, name="weight_change_dx")
height_dx = episode_relevant_window(height_query, name="height_dx")
bsa_dx = episode_relevant_window(bsa_query, name="bsa_dx")
creat_dx = episode_relevant_window(creatinine_clearance_query, name="creat_dx")
egfr_dx = episode_relevant_window(est_gfr_query, name="egfr_dx")
fev1_dx = episode_relevant_window(fev1_query, name="fev1_dx")
dtherm_dx = episode_relevant_window(distress_thermometer_query, name="dtherm_dx")
ecog_dx = episode_relevant_window(ecog_query, name="ecog_dx")
pyh_dx = episode_relevant_window(smoking_pyh_query, name="pyh_dx")

dx_all_measurements = episode_relevant_window(
    measurement_attached_to_condition_episode(
        concept_ids=None,  
        include_cols=[
            Measurement.value_as_number,
            Measurement.value_as_concept_id,
            Measurement.unit_concept_id,
        ],
        name="dx_all_measurements",
        unlinked_only=False,  
        policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    ),
    name="dx_all_measurements_windowed",
)
