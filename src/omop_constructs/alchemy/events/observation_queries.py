from .event_factories import (
    EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    episode_relevant_window,
    observation_attached_to_condition_episode,
)
from omop_alchemy.cdm.model import Observation
dx_all_observations = episode_relevant_window(
    observation_attached_to_condition_episode(
        concept_ids=None,  # all obs
        include_cols=[
            Observation.value_as_number,
            Observation.qualifier_concept_id,
            Observation.observation_concept_id.label("observation_concept_id"),
            Observation.observation_date.label("observation_date"),
        ],
        name="dx_all_observations",
        unlinked_only=False,
        policy=EVENT_CONSTRUCT_ATTACHMENT_POLICY,
    ),
    name="dx_all_observations_windowed",
)
