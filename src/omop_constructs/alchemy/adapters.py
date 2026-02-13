import sqlalchemy as sa
from typing import Type

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
    return type(
        name,
        (base,),
        {
            "__table__": subquery,
            "__mapper_args__": {
                "primary_key": [subquery.c[c] for c in pk_cols]
            },
        },
    )