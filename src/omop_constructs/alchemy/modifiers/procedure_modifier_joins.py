from datetime import datetime
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Concept, Procedure_Occurrence, Measurement
from omop_semantics.runtime.default_valuesets import runtime


modifier_concept = so.aliased(Concept, name='modifier_concept')
procedure_concept = so.aliased(Concept, name='procedure_concept')

modified_procedure_join = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_datetime, 
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_source_value,
        Procedure_Occurrence.procedure_concept_id,
        procedure_concept.concept_name.label('procedure_concept'),
        Measurement.measurement_id.label('intent_id'),
        Measurement.measurement_datetime.label('intent_datetime'),
        Measurement.measurement_concept_id.label('intent_concept_id'),
        modifier_concept.concept_name.label('intent_concept'),
        sa.func.row_number().over().label('mv_id')
    )
    .join(
        Measurement, 
        sa.and_(
            Measurement.modifier_of_field_concept_id==runtime.modifiers.modifier_fields.procedure_occurrence_id,
            Procedure_Occurrence.procedure_occurrence_id==Measurement.modifier_of_event_id,
            Measurement.measurement_concept_id.in_(runtime.treatment_modifiers.treatment_intent.ids)
        ),
        isouter=True
    )
    .join(procedure_concept, procedure_concept.concept_id==Procedure_Occurrence.procedure_concept_id)
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
)

