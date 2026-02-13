import sqlalchemy as sa
from typing import Iterable
from omop_semantics.runtime.default_valuesets import runtime
from omop_alchemy.cdm.model.clinical import Measurement, Condition_Occurrence
from omop_alchemy.cdm.model.vocabulary import Concept

def make_measurement_modifier_subset(
    *,
    concept_ids: Iterable[int],
    name: str
) -> sa.Subquery:
    """
    Build a subquery selecting Measurement rows for a given semantic modifier group.

    Parameters
    ----------
    concept_ids
        Iterable of OMOP concept_ids defining the semantic subset (e.g. T stage concepts).
    name
        Name of the resulting subquery.
    """

    cols = [
        Measurement.modifier_of_event_id.label("modifier_of_event_id"),
        Measurement.measurement_concept_id.label(f"{name}_concept_id"),
        Measurement.measurement_date.label(f"{name}_date"),
        Measurement.value_as_concept_id.label(f"{name}_value")
    ]
    return (
        sa.select(*cols)
        .where(
            sa.and_(
                Measurement.measurement_concept_id.in_(concept_ids),
                Measurement.modifier_of_field_concept_id
                == runtime.modifiers.modifier_fields.condition_occurrence_id,
            )
        )
        .subquery(name)
    )

path_stage_subquery = (
    sa.select(
        Measurement.modifier_of_event_id.label("modifier_of_event_id"),
        Measurement.measurement_concept_id.label(f"path_stage_concept_id"),
        Measurement.measurement_date.label(f"path_stage_date"),
        Measurement.value_as_concept_id.label(f"path_stage_value")
    )
    .join(Concept, Concept.concept_id == Measurement.measurement_concept_id)
    .where(
        sa.func.substring(Concept.concept_code, 1, 1) == "p"
    )
    .subquery("path_stage_subquery")
)

staging_query_specifications = {
    "t_stage": runtime.staging.t_stage_concepts.ids,
    "n_stage": runtime.staging.n_stage_concepts.ids,
    "m_stage": runtime.staging.m_stage_concepts.ids,
    "group_stage": runtime.staging.group_stage_concepts.ids,
    "grade": runtime.condition_modifiers.tumor_grade.ids,
    "laterality": [runtime.condition_modifiers.condition_modifier_values.laterality],
}

staging_subqueries = {
    name: make_measurement_modifier_subset(
        concept_ids=concept_ids,
        name=f"{name}",
    )
    for name, concept_ids in staging_query_specifications.items()
}

staging_subqueries["path_stage"] = path_stage_subquery


def make_condition_modifier_fanout(
    *,
    base_cls=Condition_Occurrence,
    modifiers: dict[str, type],
    name: str = "cancer_dx_join",
) -> sa.Subquery:
    """
    Build a wide join of Condition_Occurrence to multiple modifier views.
    """

    cols = [
        base_cls.person_id.label("person_id"),
        base_cls.condition_occurrence_id.label("cancer_diagnosis_id"),
        base_cls.condition_start_date.label("cancer_start_date"),
    ]

    q = sa.select(*cols)

    for mod_name, mod_cls in modifiers.items():
        table = mod_cls.__table__

        # choose which value column to use (concept vs value)
        value_col = table.c.get(f"{mod_name}_value")
        if value_col is None:
            value_col = table.c.get(f"{mod_name}_concept_id")

        if value_col is None:
            raise KeyError(
                f"Could not find value column for modifier '{mod_name}'. "
                f"Available columns: {list(table.c.keys())}"
            )

        date_col = table.c[f"{mod_name}_date"]

        q = q.add_columns(
            value_col.label(f"{mod_name}_value"),
            date_col.label(f"{mod_name}_date"),
        )

        q = q.join(
            mod_cls,
            mod_cls.modifier_of_event_id == base_cls.condition_occurrence_id,
            isouter=True,
        )

    return q.subquery(name)
