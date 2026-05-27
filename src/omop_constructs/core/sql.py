from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.sql.selectable import SelectBase


def select_all_columns(
    selectable: sa.FromClause | SelectBase,
) -> sa.Select:
    """
    Build ``SELECT *`` from a selectable while making any subquery boundary
    explicit for SQLAlchemy 2.x.
    """
    if isinstance(selectable, SelectBase):
        selectable = selectable.subquery()
    return sa.select(*selectable.c)
