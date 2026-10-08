"""delegate projection and ranking to omop-alchemy 
:
* ``get_eav_modifier_query`` and ``get_direct_modifier_query`` compose
  ``canonical_modifier_projection`` with a concept predicate and an outer label
  enrichment;
* ``earliest_modifier`` delegates to ``selected_modifier_select``; and
* ``get_query_per_stage_type`` delegates to ``preferred_stage_select`` with
  ``DEFAULT_STAGE_SELECTION``.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.cdm.model.clinical import Measurement
from omop_alchemy.cdm.model.vocabulary import Concept
from omop_alchemy.toolkit.analytics.oncology.condition_modifiers import (
    DEFAULT_STAGE_SELECTION,
    preferred_stage_select,
)
from omop_alchemy.toolkit.core.modifiers import (
    ModifierColumn,
    ModifierSelectionPolicy,
    ModifierSelectionSpec,
    canonical_modifier_projection,
    selected_modifier_select,
)

modifier_concept = so.aliased(Concept, name="modifier_concept")

#: Legacy label for each canonical modifier column. The canonical vocabulary
#: exists precisely so this rename happens once, visibly, at the boundary.
_LEGACY_LABELS: dict[str, str] = {
    str(ModifierColumn.person_id): "person_id",
    str(ModifierColumn.modifier_id): "measurement_id",
    str(ModifierColumn.modifier_date): "measurement_date",
    str(ModifierColumn.modifier_datetime): "measurement_datetime",
    str(ModifierColumn.modifier_concept_id): "measurement_concept_id",
    str(ModifierColumn.target_event_id): "measurement_event_id",
    str(ModifierColumn.target_field_concept_id): "meas_event_field_concept_id",
}

#: The legacy column order the modifier views select positionally.
_LEGACY_CORE_ORDER = (
    "person_id",
    "measurement_event_id",
    "meas_event_field_concept_id",
    "measurement_concept_id",
    "measurement_id",
    "measurement_date",
    "concept_name",
)

_CONCEPT_CODE = "modifier_concept_code"
_RANK_LABEL = "rn"


#: A governed concept set, either already resolved or a database-side subquery.
ConceptSet = Sequence[int] | sa.Select[Any]


def _concept_predicate(concept_ids: ConceptSet) -> sa.ColumnElement[bool]:
    subset = concept_ids if isinstance(concept_ids, sa.Select) else list(concept_ids)
    return Measurement.measurement_concept_id.in_(subset)


def _canonical_core(
    *,
    concept_ids: ConceptSet,
    label_join_column: so.InstrumentedAttribute[Any],
    extra_columns: Iterable[Any],
    extra_labels: Iterable[Any] = (),
    name: str,
) -> sa.Subquery:
    """Canonical modifier rows for one concept set, with an outer label join.

    ``include_values=False`` because the caller supplies whichever value columns
    its view exposes; requesting them here as well would emit two columns with
    the same label.
    """
    query = (
        canonical_modifier_projection(Measurement, include_values=False)
        .add_columns(
            modifier_concept.concept_name.label("concept_name"),
            *extra_labels,
            *extra_columns,
        )
        .join(
            modifier_concept,
            modifier_concept.concept_id == label_join_column,
            isouter=True,
        )
        .where(_concept_predicate(concept_ids))
    )
    return query.subquery(name=name)


def _to_legacy(
    core: sa.Subquery, *, extra_keys: Sequence[str], name: str
) -> sa.Subquery:
    """Rename canonical columns to the labels the deployed views select."""
    ordered: dict[str, sa.ColumnElement[Any]] = {
        legacy: core.c[canonical].label(legacy)
        for canonical, legacy in _LEGACY_LABELS.items()
        if legacy in _LEGACY_CORE_ORDER and canonical in core.c
    }
    # Already carries its legacy name, so it needs no relabelling.
    ordered["concept_name"] = core.c["concept_name"]
    return (
        sa.select(
            *(ordered[label] for label in _LEGACY_CORE_ORDER),
            *(core.c[key] for key in extra_keys),
        )
        .subquery(name=name)
    )


def get_eav_modifier_query(
    modifier_concept_id: int,
    target_cols: Iterable[so.InstrumentedAttribute[Any]] = (
        Measurement.value_as_concept_id,
    ),
    join_col: so.InstrumentedAttribute[Any] = Measurement.value_as_concept_id,
    name: str = "eav_modifier",
) -> sa.Subquery:
    """Modifiers of one concept, labelled by the concept their *value* names.

    The EAV shape carries its meaning in the value, so ``concept_name`` is the
    label of ``join_col`` (the value concept) rather than of the modifier
    concept itself. That is preserved exactly.
    """
    extras = list(target_cols)
    core = _canonical_core(
        concept_ids=[modifier_concept_id],
        label_join_column=join_col,
        extra_columns=extras,
        name=f"{name}_canonical",
    )
    return _to_legacy(
        core, extra_keys=[column.key for column in extras], name=name
    )


def get_direct_modifier_query(
    modifier_concept_id: ConceptSet,
    target_cols: Iterable[so.InstrumentedAttribute[Any]] = (),
    name: str = "direct_modifier",
) -> sa.Subquery:
    """Modifiers drawn from a concept set, labelled by the modifier concept."""
    extras = list(target_cols)
    core = _canonical_core(
        concept_ids=modifier_concept_id,
        label_join_column=Measurement.measurement_concept_id,
        extra_columns=extras,
        name=f"{name}_canonical",
    )
    return _to_legacy(
        core, extra_keys=[column.key for column in extras], name=name
    )


def _legacy_selection_spec(source: sa.Subquery) -> ModifierSelectionSpec:
    """Selection over legacy labels, partitioned on the whole target identity."""
    has_datetime = "measurement_datetime" in source.c
    return ModifierSelectionSpec(
        policy=ModifierSelectionPolicy.earliest,
        partition_by=(
            "person_id",
            "meas_event_field_concept_id",
            "measurement_event_id",
        ),
        date_column="measurement_date",
        datetime_column=(
            "measurement_datetime" if has_datetime else "measurement_date"
        ),
        stable_identity_columns=("measurement_id",),
    )


def earliest_modifier(
    starting_query: sa.Subquery, name: str = "earliest_modifier"
) -> sa.Subquery:
    """Select the earliest modifier per target, deterministically."""
    output_keys = [column.key for column in starting_query.c]
    ranked_source = sa.select(
        *starting_query.c,
        starting_query.c["measurement_event_id"].label(
            str(ModifierColumn.target_event_id)
        ),
        starting_query.c["meas_event_field_concept_id"].label(
            str(ModifierColumn.target_field_concept_id)
        ),
    ).subquery(name=f"{name}_target_scoped")

    selected = selected_modifier_select(
        ranked_source, spec=_legacy_selection_spec(starting_query)
    ).subquery(name=f"{name}_selected")

    # Every surviving row is the winner of its partition, so the legacy rank
    # column is a constant rather than a second window function.
    return sa.select(
        *(selected.c[key] for key in output_keys),
        sa.literal(1).label(_RANK_LABEL),
    ).subquery(name=f"{name}_filtered")


def get_query_per_stage_type(
    subset: ConceptSet, name: str = "stage_modifier"
) -> sa.Subquery:
    """Select one preferred stage per target: pathological first, then earliest."""
    core = _canonical_core(
        concept_ids=subset,
        label_join_column=Measurement.measurement_concept_id,
        extra_columns=(),
        extra_labels=(modifier_concept.concept_code.label(_CONCEPT_CODE),),
        name=f"{name}_canonical",
    )
    selected = preferred_stage_select(
        core, spec=DEFAULT_STAGE_SELECTION, concept_code_column=_CONCEPT_CODE
    ).subquery(name=f"{name}_selected")

    # stage_type is retained only because the deployed view exposes it. It is a
    # rendering of the same p/c basis that preferred_stage_select ranked on, not
    # a second, independent rule.
    stage_type = sa.case(
        (
            sa.func.lower(sa.func.trim(selected.c[_CONCEPT_CODE])).like("p%"),
            sa.literal("aaa_path"),
        ),
        else_=sa.literal("zzz_clin"),
    ).label("stage_type")

    return sa.select(
        selected.c[str(ModifierColumn.person_id)].label("person_id"),
        selected.c[str(ModifierColumn.modifier_id)].label("stage_id"),
        selected.c[str(ModifierColumn.modifier_date)].label("stage_date"),
        selected.c[str(ModifierColumn.modifier_datetime)].label("stage_datetime"),
        selected.c[str(ModifierColumn.modifier_concept_id)].label("stage_concept_id"),
        selected.c[str(ModifierColumn.target_event_id)].label("measurement_event_id"),
        selected.c[str(ModifierColumn.target_field_concept_id)].label(
            "meas_event_field_concept_id"
        ),
        selected.c["concept_name"].label("stage_label"),
        stage_type,
        sa.literal(1).label(_RANK_LABEL),
    ).subquery(name=f"{name}_filtered")
