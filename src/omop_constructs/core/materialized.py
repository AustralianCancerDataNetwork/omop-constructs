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
    __mv_index__: str | None = None  # Optional column name to create an index on
    __construct__ = type("Construct", (), {"kind": "materialized_view"})

    @classmethod
    def create_mv(cls, bind, *, with_data: bool = True) -> None:
        ddl = CreateMaterializedView(cls.__mv_name__, cls.__mv_select__, with_data=with_data)
        bind.execute(ddl)
        if cls.__mv_index__ is not None:
            cls.create_index(bind, index_colname=cls.__mv_index__)

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

    @classmethod
    def create_index(cls, bind, index_colname: str):
        bind.execute(sa.text(
            f"CREATE INDEX IF NOT EXISTS {cls.__mv_name__}_{index_colname}_idx "
            f"ON {cls.__mv_name__} ({index_colname})"
        ))
