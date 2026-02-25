from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from .episode_factories import get_episode_query, get_episode_hierarchy_query, dx_treatment_window

episode_of_care_select = get_episode_query(
        [runtime.types.disease_episode_types.episode_of_care],  # type: ignore
        name="episode_of_care"
    )

disease_extent_select = get_episode_query(
        [runtime.types.disease_episode_types.disease_progression, runtime.types.disease_episode_types.metastatic],  # type: ignore
        name="disease_extent"
    )

overarching_disease_episode = get_episode_hierarchy_query(
    episode_of_care_select,
    disease_extent_select,
    name="overarching_disease_episode",
)

condition_episode_select = get_episode_query(
    [runtime.types.disease_episode_types.disease_progression, runtime.types.disease_episode_types.metastatic, runtime.types.disease_episode_types.episode_of_care],  # type: ignore
    name="condition_episode",
)

treatment_regimen_select = get_episode_query(
    [runtime.types.treatment_episode_types.treatment_regimen],  # type: ignore
    name="treatment_regimen",
)

treatment_cycle_select = get_episode_query(
    [runtime.types.treatment_episode_types.treatment_cycle],  # type: ignore
    name="treatment_cycle",
)

treatment_regimen_with_cycles = get_episode_hierarchy_query(
    parent_episode_subq=treatment_regimen_select,
    child_episode_subq=treatment_cycle_select,
    name="treatment_regimen_with_cycles",
    child_label="cycle_episode"
)

dx_window_select = dx_treatment_window()