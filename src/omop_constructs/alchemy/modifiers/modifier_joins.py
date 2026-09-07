"""Query fragments for staging and condition modifier selection.

Importing this module performs no database work. Concept sets are expressed as
database-side subqueries over ``concept_ancestor`` rather than resolved into
literal ID lists at import time (CM-H2), which also makes the compiled SQL
reproducible: a resolved Python set renders in hash-seed-dependent order, so the
same version built twice produced different definition checksums (OC-0-N10).
"""

from __future__ import annotations

from typing import Any, Iterable

import sqlalchemy as sa

from omop_alchemy.cdm.model.clinical import Measurement
from omop_alchemy.toolkit.core.concepts import descendant_concept_select
from omop_semantics.runtime.default_valuesets import runtime

from .modifier_factories import (
    earliest_modifier,
    get_direct_modifier_query,
    get_eav_modifier_query,
    get_query_per_stage_type,
)

_STAGING = runtime.staging
_MODIFIERS = runtime.condition_modifiers


def _governed(*parent_sets: Iterable[Any]) -> sa.Select[Any]:
    """Standard descendants of the given governed parents.

    ``require_standard`` and ``include_classification`` match the defaults the
    Constructs resolver registry used, so set membership is unchanged.
    """
    parents = [int(value) for parents in parent_sets for value in parents]
    return descendant_concept_select(
        parents, require_standard=True, include_classification=True
    )


_T_STAGE = _STAGING.t_stage_concepts.parent_ids
_N_STAGE = _STAGING.n_stage_concepts.parent_ids
_M_STAGE = _STAGING.m_stage_concepts.parent_ids
_GROUP_STAGE = _STAGING.group_stage_concepts.parent_ids

t_stage_select = get_query_per_stage_type(_governed(_T_STAGE), name="t_stage")
n_stage_select = get_query_per_stage_type(_governed(_N_STAGE), name="n_stage")
m_stage_select = get_query_per_stage_type(_governed(_M_STAGE), name="m_stage")
group_stage_select = get_query_per_stage_type(
    _governed(_GROUP_STAGE), name="group_stage"
)

laterality_select = earliest_modifier(
    get_eav_modifier_query(
        _MODIFIERS.condition_modifier_values.laterality,
        name="tumor_laterality",
    ),
    name="tumor_laterality_earliest",
)

size_select = earliest_modifier(
    get_eav_modifier_query(
        _MODIFIERS.numeric_condition_modifiers.tumor_size,
        target_cols=[Measurement.value_as_number, Measurement.unit_concept_id],
        join_col=Measurement.unit_concept_id,
        name="tumor_size",
    ),
    name="tumor_size_earliest",
)

grade_select = earliest_modifier(
    get_direct_modifier_query(
        _governed(_MODIFIERS.tumor_grade.exact_ids), name="tumor_grade"
    ),
    name="tumor_grade_earliest",
)

mets_select = earliest_modifier(
    get_direct_modifier_query(
        _governed((_MODIFIERS.condition_modifier_values.metastatic_disease,)),
        target_cols=[Measurement.value_as_concept_id],
        name="metastatic_disease",
    ),
    name="metastatic_disease_earliest",
)

# Long-form: every stage measurement of any type, deliberately unranked.
all_stage_select = get_direct_modifier_query(
    _governed(_T_STAGE, _N_STAGE, _M_STAGE, _GROUP_STAGE),
    name="all_stage_modifiers",
)
