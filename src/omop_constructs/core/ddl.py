from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles
import sqlalchemy as sa

class CreateMaterializedView(DDLElement):
    """
    SQLAlchemy DDL element for CREATE MATERIALIZED VIEW IF NOT EXISTS.
    """
    def __init__(
        self,
        name: str,
        selectable: sa.sql.Select,
        *,
        with_data: bool = True,
        schema_translate_map: dict[str, str | None] | None = None,
    ):
        self.name = name
        self.selectable = selectable
        self.with_data = with_data
        self.schema_translate_map = schema_translate_map

@compiles(CreateMaterializedView)
def _create_mv(element: CreateMaterializedView, compiler, **kw):
    schema_translate_map = (
        element.schema_translate_map
        or kw.get("schema_translate_map")
        or compiler.schema_translate_map
    )
    compiled = str(
        element.selectable.compile(
            dialect=compiler.dialect,
            compile_kwargs={"literal_binds": True},
            schema_translate_map=schema_translate_map,
        )
    )
    for role, schema in (schema_translate_map or {}).items():
        tag = getattr(role, "value", role)
        placeholder = f"__[SCHEMA_{tag}]"
        if schema:
            compiled = compiled.replace(placeholder, compiler.preparer.quote_schema(schema))
        else:
            compiled = compiled.replace(f"{placeholder}.", "")
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
