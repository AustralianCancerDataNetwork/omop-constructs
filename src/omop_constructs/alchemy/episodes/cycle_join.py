import sqlalchemy as sa
from omop_semantics.runtime.default_valuesets import runtime
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Concept, Drug_Exposure, Episode, Episode_Event

drug_concept = so.aliased(Concept, name='drug_concept')
route_concept = so.aliased(Concept, name='route_concept')
cycle_concept = so.aliased(Concept, name='cycle_concept')

cycle_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Drug_Exposure.person_id,
        Drug_Exposure.drug_exposure_start_date,
        Drug_Exposure.drug_exposure_end_date,
        Drug_Exposure.drug_exposure_id,
        Drug_Exposure.quantity,
        Drug_Exposure.drug_concept_id,
        Drug_Exposure.dose_unit_source_value,
        drug_concept.concept_name.label('drug_name'),
        route_concept.concept_name.label('route'),
        Episode.episode_id.label('cycle_id'),
        Episode.episode_number.label('cycle_number'),
        Episode.episode_parent_id.label('regimen_id'),
        cycle_concept.concept_name.label('cycle_concept')
    )
    .join(drug_concept, drug_concept.concept_id==Drug_Exposure.drug_concept_id)
    .join(route_concept, route_concept.concept_id==Drug_Exposure.route_concept_id)
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Drug_Exposure.drug_exposure_id,
            Episode_Event.episode_event_field_concept_id==runtime.modifiers.modifier_fields.drug_exposure_id
        )
    )
    .join(Episode, Episode.episode_id==Episode_Event.episode_id)
    .join(cycle_concept, cycle_concept.concept_id==Episode.episode_concept_id)
)
