from .event_factories import procedure_attached_to_condition_episode, episode_relevant_window
from omop_alchemy.cdm.model import Procedure_Occurrence

dx_all_procedures = episode_relevant_window(
    procedure_attached_to_condition_episode(
        concept_ids=None,
        include_cols=[
            Procedure_Occurrence.procedure_concept_id.label("procedure_concept_id"),
            Procedure_Occurrence.procedure_datetime.label("procedure_datetime"),
        ],
        name="dx_all_procedures",
    ),
    name="dx_all_procedures_windowed",
)
