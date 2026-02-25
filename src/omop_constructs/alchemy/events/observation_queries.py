from .event_factories import observation_attached_to_condition_episode, episode_relevant_window
from omop_alchemy.cdm.model import Observation
dx_all_observations = episode_relevant_window(
    observation_attached_to_condition_episode(
        concept_ids=None,  # all obs
        include_cols=[
            Observation.value_as_number,
            Observation.qualifier_concept_id,
        ],
        name="dx_all_observations",
        unlinked_only=False,
    ),
    max_days_post=365,
    max_days_prior=30,
    name="dx_all_observations_windowed",
)
