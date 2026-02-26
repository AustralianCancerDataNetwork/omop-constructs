import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Episode, Episode_Event, Concept
from omop_semantics.runtime.default_valuesets import runtime
from ..modifiers import ModifiedProcedure
from .fraction_mv import FractionMV


course_concept = so.aliased(Concept, name='course_concept')

frac_summary_join = (
    sa.select(
        FractionMV.person_id,
        FractionMV.fraction_id,
        FractionMV.fraction_number,
        FractionMV.course_id,
        FractionMV.fraction_name,
        sa.func.min(FractionMV.procedure_datetime).label('first_exposure_date'),
        sa.func.max(FractionMV.procedure_datetime).label('last_exposure_date'),
        sa.func.count(FractionMV.procedure_occurrence_id).label('fraction_count'),
    )
    .group_by(FractionMV.person_id, FractionMV.fraction_id, FractionMV.fraction_number, FractionMV.course_id, FractionMV.fraction_name)
    .subquery()
)

course_join = (
    sa.select(
        *frac_summary_join.c,
        sa.func.row_number().over().label('mv_id'),
        Episode.episode_number.label('course_number'),
        Episode.episode_parent_id.label('condition_episode_id'),
        ModifiedProcedure.procedure_occurrence_id.label('course_prescription_id'),
        ModifiedProcedure.procedure_concept_id.label('course_concept_id'),
        ModifiedProcedure.procedure_concept.label('course_concept'),
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime
    )
    .join(
        Episode, 
        sa.and_(
            Episode.episode_id == frac_summary_join.c.course_id, 
            Episode.episode_concept_id==runtime.types.treatment_episode_types.treatment_regimen
        ),
        isouter=True
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.episode_id == Episode.episode_id, 
            Episode_Event.episode_event_field_concept_id == runtime.modifiers.modifier_fields.procedure_occurrence_id
        ),
        isouter=True
    )
    .join(ModifiedProcedure, ModifiedProcedure.procedure_occurrence_id==Episode_Event.event_id, isouter=True)
)