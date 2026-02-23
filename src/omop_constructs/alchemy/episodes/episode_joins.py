from .episode_factories import require_condition_anchor, get_episode_query, get_episode_hierarchy_query
from omop_semantics.runtime.default_valuesets import runtime

episode_of_care_select = require_condition_anchor(
    get_episode_query(
        [runtime.types.disease_episode_types.episode_of_care],  # type: ignore
        name="episode_of_care"
    )
)

disease_extent_select = require_condition_anchor(
    get_episode_query(
        [runtime.types.disease_episode_types.disease_progression, runtime.types.disease_episode_types.metastatic],  # type: ignore
        name="disease_extent"
    )
)   

overarching_disease_episode = get_episode_hierarchy_query(
    episode_of_care_select,
    disease_extent_select,
    name="overarching_disease_episode",
)