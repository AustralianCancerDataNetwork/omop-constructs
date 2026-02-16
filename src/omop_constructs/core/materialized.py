import sqlalchemy as sa
from .ddl import CreateMaterializedView, DropMaterializedView


class MaterializedViewMixin:
    """
    Mixin for declarative SQLAlchemy models backed by materialized views.

    Requires:
      - __mv_name__: str
      - __mv_select__: sqlalchemy.sql.Select
    """

    __mv_name__: str
    __mv_select__: sa.sql.Select

    @classmethod
    def create_mv(cls, bind):
        ddl = CreateMaterializedView(cls.__mv_name__, cls.__mv_select__)
        bind.execute(ddl)

    @classmethod
    def refresh_mv(cls, bind, *, concurrently: bool = False):
        suffix = " CONCURRENTLY" if concurrently else ""
        bind.execute(
            sa.text(f"REFRESH MATERIALIZED VIEW{suffix} {cls.__mv_name__}")
        )

    @classmethod
    def drop_mv(cls, bind, *, cascade: bool = False):
        ddl = DropMaterializedView(cls.__mv_name__, cascade=cascade)
        bind.execute(ddl)