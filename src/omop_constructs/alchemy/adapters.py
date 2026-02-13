import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Type
from .source_factories import make_source_lookup
from sqlalchemy.sql.type_api import TypeEngine

def map_lookup_view(
    *,
    base: Type,
    subquery: sa.Subquery,
    name: str,
    pk_cols: list[str],
):
    """
    Create a mapped class over a subquery.
    """

    missing = [c for c in pk_cols if c not in subquery.c]
    if missing:
        raise KeyError(
            f"Columns {missing} not found in subquery {subquery.name or '<anon>'}. "
            f"Available columns: {list(subquery.c.keys())}"
        )

    return type(
        name,
        (base,),
        {
            "__table__": subquery,
            "__mapper_args__": {
                "primary_key": [subquery.c[c] for c in pk_cols]
            },
            "__annotations__": {
                col.name: so.Mapped[object] for col in subquery.c
            },
        },
    )

def make_and_map_lookup_view(
    *,
    base: Type,
    name: str,
    model: Type,
    source_column: sa.Column,
    source_label: str,
    cast_to: TypeEngine | None = None,
    pk_cols: list[str],
):
    subq = make_source_lookup(
        model=model,
        source_column=source_column,
        source_label=source_label,
        cast_to=cast_to,
        name=name.lower(),
    )
    return map_lookup_view(
        base=base,
        subquery=subq,
        name=name,
        pk_cols=pk_cols,
    )