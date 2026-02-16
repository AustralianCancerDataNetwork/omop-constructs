import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Iterable

from omop_semantics.runtime.default_valuesets import runtime
from omop_alchemy.cdm.model.clinical import Measurement
from omop_alchemy.cdm.model.vocabulary import Concept

modifier_concept = so.aliased(Concept, name='modifier_concept')

def get_eav_modifier_query(
        modifier_concept_id: int, 
        target_cols: Iterable[so.InstrumentedAttribute] = [Measurement.value_as_concept_id], 
        join_col: so.InstrumentedAttribute = Measurement.value_as_concept_id,
        name: str = "eav_modifier"
) -> sa.Subquery:
    return (
        sa.select(
            Measurement.person_id,
            Measurement.measurement_event_id,
            Measurement.meas_event_field_concept_id,
            Measurement.measurement_concept_id, 
            Measurement.measurement_id, 
            Measurement.measurement_date, 
            modifier_concept.concept_name,
            *target_cols
        )
        .join(modifier_concept, modifier_concept.concept_id==join_col, isouter=True)
        .filter(Measurement.measurement_concept_id==modifier_concept_id)
        .subquery(name = name)
    )

def get_direct_modifier_query(
        modifier_concept_id: list[int],
        name: str = "direct_modifier"
) -> sa.Subquery:
    return (
        sa.select(
            Measurement.person_id,
            Measurement.measurement_event_id,
            Measurement.meas_event_field_concept_id,
            Measurement.measurement_concept_id,
            Measurement.measurement_id, 
            Measurement.measurement_date, 
            modifier_concept.concept_name,
        )
        .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
        .filter(Measurement.measurement_concept_id.in_(modifier_concept_id))
        .subquery(name = name)
    )

def earliest_modifier(
        starting_query: sa.Subquery,
        name: str = "earliest_modifier"
) -> sa.Subquery:
    ranked = (
        sa.select(
            *starting_query.c,
            sa.func.row_number()
            .over(
                partition_by=starting_query.c.measurement_event_id,
                order_by=starting_query.c.measurement_date.asc()
            )
            .label('rn')
        ).subquery(name=name)
    )
    return sa.select(*ranked.c).where(ranked.c.rn==1).subquery(name=f"{name}_filtered")

def get_query_per_stage_type(
        subset: Iterable[int],
        name: str = "stage_modifier"
) -> sa.Subquery:
    """
    Build a subquery for a specific stage modifier 

    Ranks multiple modifiers per diagnosis and selects the most 
    relevant one based on predefined rules.

    T, N, M and Group stage queries preference *earliest pathological  
    stage* if it exists else fall back to earliest clinical stage.
    """
    stage_select = (

        sa.select(
            Measurement.person_id,
            Measurement.measurement_id.label("stage_id"),
            Measurement.measurement_date.label("stage_date"),
            Measurement.measurement_datetime.label("stage_datetime"),
            Measurement.measurement_concept_id.label("stage_concept_id"),
            Measurement.measurement_event_id,
            Measurement.meas_event_field_concept_id,
            modifier_concept.concept_name.label("stage_label"),
            sa.case(
                (
                    modifier_concept.concept_code.like("p%"),
                    sa.literal("aaa_path"),
                ),
                else_=sa.literal("zzz_clin"),
            ).label("stage_type"),
        )
        .join(
            modifier_concept,
            modifier_concept.concept_id == Measurement.measurement_concept_id,
            isouter=True,
        )
    )
    i = (
        stage_select
        .filter(Measurement.measurement_concept_id.in_(subset))
        .subquery(name=f"{name}_initial")
    )

    ranked = (
        sa.select(
            *i.c,
            sa.func.row_number()
            .over(
                partition_by=i.c.measurement_event_id,
                order_by=[i.c.stage_type, i.c.stage_date.asc()],
            )
            .label("rn"),
        )
        .subquery(name=f"{name}_ranked")
    )

    return sa.select(*ranked.c).where(ranked.c.rn == 1).subquery(name=f'{name}_filtered')
