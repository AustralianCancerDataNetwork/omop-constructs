from .event_factories import procedure_attached_to_condition_episode, episode_relevant_window

dx_all_procedures = episode_relevant_window(
    procedure_attached_to_condition_episode(
        concept_ids=None,
        include_cols=[],
        name="dx_all_procedures",
    ),
    max_days_post=90,
    max_days_prior=30,
    name="dx_all_procedures_windowed",
)
