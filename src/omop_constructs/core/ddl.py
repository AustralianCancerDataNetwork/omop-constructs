from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles
import sqlalchemy as sa

class CreateMaterializedView(DDLElement):
    """
    SQLAlchemy DDL element for CREATE MATERIALIZED VIEW IF NOT EXISTS.
    """
    def __init__(self, name: str, selectable: sa.sql.Select, *, with_data: bool = True):
        self.name = name
        self.selectable = selectable
        self.with_data = with_data

@compiles(CreateMaterializedView)
def _create_mv(element: CreateMaterializedView, compiler, **kw):
    compiled = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    with_data = "WITH DATA" if element.with_data else "WITH NO DATA"
    return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {element.name} AS {compiled} {with_data}"

class DropMaterializedView(DDLElement):
    def __init__(self, name: str, *, cascade: bool = False):
        self.name = name
        self.cascade = cascade

@compiles(DropMaterializedView)
def _drop_mv(element: DropMaterializedView, compiler, **kw):
    cascade = " CASCADE" if element.cascade else ""
    return f"DROP MATERIALIZED VIEW IF EXISTS {element.name}{cascade}"
