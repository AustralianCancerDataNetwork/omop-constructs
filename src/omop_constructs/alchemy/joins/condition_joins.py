
from collections import defaultdict
import sqlalchemy.orm as so
import sqlalchemy as sa
from omop_alchemy.cdm.model.clinical import Condition_Occurrence

condition_window = (
    sa.select(
        Condition_Occurrence.person_id, 
        sa.func.min(Condition_Occurrence.condition_start_date).label('first_dx'),
        sa.func.max(Condition_Occurrence.condition_start_date).label('last_dx')
    )
    .group_by(Condition_Occurrence.person_id)
    .subquery()
)


condition_source_lookup = (
    sa.select(
        Condition_Occurrence.person_id, 
        Condition_Occurrence.condition_occurrence_id,
        Condition_Occurrence.condition_concept_id,
        Condition_Occurrence.condition_start_date,
        Condition_Occurrence.condition_end_date,
        sa.cast(Condition_Occurrence.condition_status_source_value, sa.Integer).label('med_id')
    )
    .subquery()
)

