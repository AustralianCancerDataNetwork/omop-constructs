from typing import Type
import sqlalchemy as sa
from sqlalchemy.sql.type_api import TypeEngine
from typing import Protocol, runtime_checkable

@runtime_checkable
class HasPersonIdModel(Protocol):
    person_id: sa.Column
    __table__: sa.Table

def make_source_lookup(
    *,
    model: Type[HasPersonIdModel],
    source_column: sa.Column,
    source_label: str = "source_id",
    cast_to: TypeEngine | None = None,
    name: str | None = None,
):
    """
    Build a canonical 'source lookup' subquery for a given OMOP table.

    Parameters
    ----------
    model
        ORM model (e.g. Condition_Occurrence)
    source_column
        Column on the model to treat as the source identifier
    source_label
        Output column name for the source id
    cast_to
        Optional SQLAlchemy type to cast the source column to
    name
        Optional subquery name
    """
    cols = [
        model.person_id,
        model.__table__.primary_key.columns.values()[0],  # condition_occurrence_id, etc
    ]

    # Optional standard OMOP columns if present
    for col_name in ["condition_concept_id", "condition_start_date", "condition_end_date"]:
        if hasattr(model, col_name):
            cols.append(getattr(model, col_name))

    source_expr = source_column
    if cast_to is not None:
        source_expr = sa.cast(source_expr, cast_to)

    cols.append(source_expr.label(source_label))

    return sa.select(*cols).subquery(name=name)
