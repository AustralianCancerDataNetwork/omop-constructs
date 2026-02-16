from omop_constructs.semantics import registry
from omop_alchemy.cdm.model.clinical import Measurement
from omop_semantics.runtime.default_valuesets import runtime
from .modifier_factories import get_query_per_stage_type, get_eav_modifier_query, earliest_modifier, get_direct_modifier_query

"""
These calls have side effects of building the resolvers and querying the database to 
cache the required concepts in memory, but this is necessary to create the correct 
subqueries for staging modifiers. 

TODO: we may want to refactor to separate out the resolver construction from the concept retrieval?
"""
t_stage_select = get_query_per_stage_type(registry['tnm_t_stage'].all_concepts, name="t_stage")
n_stage_select = get_query_per_stage_type(registry['tnm_n_stage'].all_concepts, name="n_stage")
m_stage_select = get_query_per_stage_type(registry['tnm_m_stage'].all_concepts, name="m_stage")
group_stage_select = get_query_per_stage_type(registry['tnm_group_stage'].all_concepts, name="group_stage")

laterality_select = earliest_modifier(
    get_eav_modifier_query(
        runtime.condition_modifiers.condition_modifier_values.laterality, # type: ignore
        name="tumor_laterality"
    ),
    name="tumor_laterality_earliest"
)

size_select = earliest_modifier(
    get_eav_modifier_query(
        runtime.condition_modifiers.numeric_condition_modifiers.tumor_size, # type: ignore
        target_cols=[Measurement.value_as_number, Measurement.unit_concept_id], 
        join_col=Measurement.unit_concept_id,
        name="tumor_size"
    ),
    name="tumor_size_earliest"
)

grade_select = earliest_modifier(
    get_direct_modifier_query(list(registry['tumor_grade'].all_concepts), name="tumor_grade"),
    name="tumor_grade_earliest"
)


# def make_condition_modifier_fanout(
#     *,
#     base_cls=Condition_Occurrence,
#     modifiers: dict[str, type],
#     name: str = "cancer_dx_join",
# ) -> sa.Subquery:
#     """
#     Build a wide join of Condition_Occurrence to multiple modifier views.
#     """

#     cols = [
#         base_cls.person_id.label("person_id"),
#         base_cls.condition_occurrence_id.label("cancer_diagnosis_id"),
#         base_cls.condition_start_date.label("cancer_start_date"),
#     ]

#     q = sa.select(*cols)

#     for mod_name, mod_cls in modifiers.items():
#         table = mod_cls.__table__

#         # choose which value column to use (concept vs value)
#         value_col = table.c.get(f"{mod_name}_value")
#         if value_col is None:
#             value_col = table.c.get(f"{mod_name}_concept_id")

#         if value_col is None:
#             raise KeyError(
#                 f"Could not find value column for modifier '{mod_name}'. "
#                 f"Available columns: {list(table.c.keys())}"
#             )

#         date_col = table.c[f"{mod_name}_date"]

#         q = q.add_columns(
#             value_col.label(f"{mod_name}_value"),
#             date_col.label(f"{mod_name}_date"),
#         )

#         q = q.join(
#             mod_cls,
#             mod_cls.modifier_of_event_id == base_cls.condition_occurrence_id,
#             isouter=True,
#         )

#     return q.subquery(name)
