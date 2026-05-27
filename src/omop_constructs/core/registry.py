
from dataclasses import dataclass
from typing import Iterable, Iterator, Type
import sqlalchemy as sa
import json
from datetime import datetime
from sqlalchemy.dialects import postgresql
from .plan import ConstructNode, topo_sort
from .ddl import CreateMaterializedView
from .errors import ConstructSpecError
from ..typing import SupportsMaterializedView

@dataclass(frozen=True)
class ConstructPlanItem:
    """
    Serializable view of a construct's name, kind, and declared dependencies.
    """
    name: str
    kind: str
    deps: tuple[str, ...]


def materialized_view_exists(bind, name: str, schema: str = "public") -> bool:
    """
    Check whether a PostgreSQL materialized view already exists.
    """
    sql = sa.text("""
        SELECT EXISTS (
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = :schema
              AND matviewname = :name
        )
    """)
    return bool(bind.execute(sql, {"schema": schema, "name": name}).scalar())

class ConstructRegistry:
    """
    Registry of materialized views / constructs.

    Handles creation, refresh, and teardown in a controlled way.
    """
    _constructs: dict[str, Type[SupportsMaterializedView]]

    def __init__(self, constructs: Iterable[type[SupportsMaterializedView]]):
        """
        Build a registry over an iterable of construct classes.
        """
        self._constructs = {c.__mv_name__: c for c in constructs}
        
    def __iter__(self) -> Iterator[type[SupportsMaterializedView]]:
        return iter(self._constructs.values())

    def get(self, name: str) -> type[SupportsMaterializedView]:
        """
        Return a registered construct class by materialized view name.
        """
        return self._constructs[name]

    def plan(self) -> tuple[ConstructPlanItem, ...]:
        """
        Return constructs in dependency order.
        """
        nodes = [
            ConstructNode(name=k, deps=v.__deps__, kind=getattr(v.__construct__, "kind", "materialized_view"))
            for k, v in self._constructs.items()
        ]
        ordered = topo_sort(nodes)
        return tuple(ConstructPlanItem(n.name, n.kind, n.deps) for n in ordered)

    def compile_check(
        self,
        *,
        dialect: sa.engine.Dialect | None = None,
        literal_binds: bool = True,
        require_internal_deps: bool = True,
    ) -> str:
        """
        Compile every construct definition and validate basic registry metadata.

        This is an explicit, non-executing validation pass intended for tests
        and maintenance checks. It compiles each MV select/DDL in dependency
        order and checks that:

        - declared dependencies resolve within the registry
        - ORM-declared columns are present in the MV selectable
        - string-based helper columns such as ``__mv_index__`` and ``__mv_pk__``
          refer to real columns on both the mapped class and the selectable
        """
        dialect = dialect or postgresql.dialect()
        lines = ["<ConstructRegistry compile_check>"]
        known = set(self._constructs)
        errors: list[str] = []

        for item in self.plan():
            cls = self._constructs[item.name]
            name = cls.__mv_name__

            missing_deps = tuple(dep for dep in cls.__deps__ if dep not in known)
            if require_internal_deps and missing_deps:
                errors.append(f"{name}: unknown deps {list(missing_deps)}")
                lines.append(f"  ✗ {name}: unknown deps {list(missing_deps)}")
                continue

            try:
                sql = str(
                    cls.__mv_select__.compile(
                        dialect=dialect,
                        compile_kwargs={"literal_binds": literal_binds},
                    )
                )
                ddl = str(
                    CreateMaterializedView(
                        cls.__mv_name__,
                        cls.__mv_select__,
                    ).compile(dialect=dialect)
                )
            except Exception as exc:
                errors.append(f"{name}: compile failed ({exc.__class__.__name__}: {exc})")
                lines.append(f"  ✗ {name}: compile failed ({exc.__class__.__name__})")
                continue

            orm_cols = tuple(cls.__table__.columns.keys())
            selectable_cols = tuple(cls.__mv_select__.subquery().c.keys())
            orm_colset = set(orm_cols)
            selectable_colset = set(selectable_cols)

            missing_select_cols = sorted(orm_colset - selectable_colset)
            if missing_select_cols:
                errors.append(
                    f"{name}: mapped columns missing from selectable {missing_select_cols}"
                )
                lines.append(
                    f"  ✗ {name}: selectable missing mapped columns {missing_select_cols}"
                )
                continue

            index_name = getattr(cls, "__mv_index__", None)
            if index_name is not None:
                if index_name not in orm_colset or index_name not in selectable_colset:
                    errors.append(f"{name}: invalid __mv_index__ '{index_name}'")
                    lines.append(f"  ✗ {name}: invalid __mv_index__ '{index_name}'")
                    continue

            pk_names = tuple(getattr(cls, "__mv_pk__", ()) or ())
            bad_pks = sorted(
                pk_name
                for pk_name in pk_names
                if pk_name not in orm_colset or pk_name not in selectable_colset
            )
            if bad_pks:
                errors.append(f"{name}: invalid __mv_pk__ columns {bad_pks}")
                lines.append(f"  ✗ {name}: invalid __mv_pk__ columns {bad_pks}")
                continue

            sql_kb = round(len(sql) / 1024, 1)
            lines.append(
                f"  ✓ {name:25s} sql≈{sql_kb:6.1f} KB cols={len(selectable_cols)} ddl={len(ddl)} chars"
            )

        if errors:
            detail_lines = ["", "<ConstructRegistry compile_check details>"]
            detail_lines.extend(f"  - {error}" for error in errors)
            raise ConstructSpecError("\n".join(lines + detail_lines))

        return "\n".join(lines)
    
    def create_all(self, bind, *, with_data: bool = True):
        """
        Create every registered materialized view in dependency order.
        """
        for item in self.plan():
            cls = self._constructs[item.name]
            cls.create_mv(bind, with_data=with_data)

    def refresh_all(self, bind, *, concurrently: bool = False) -> None:
        for item in self.plan():
            self._constructs[item.name].refresh_mv(bind, concurrently=concurrently)

    def drop_all(self, bind, *, cascade: bool = False) -> None:
        """
        Drop every registered materialized view in reverse dependency order.
        """
        # drop reverse order
        for item in reversed(self.plan()):
            try:
                self._constructs[item.name].drop_mv(bind, cascade=cascade)
            except Exception as e:
                print(f"Error dropping {item.name}: {e}")

    def create_missing(self, bind, *, with_data: bool = True, schema: str = "public") -> list[str]:
        """
        Create only materialized views that do not yet exist.

        Returns a list of MV names that were created.
        """
        created: list[str] = []

        for item in self.plan():
            cls = self._constructs[item.name]

            exists = materialized_view_exists(bind, cls.__mv_name__, schema=schema)
            if exists:
                continue

            cls.create_mv(bind, with_data=with_data)
            created.append(cls.__mv_name__)

        return created

    def refresh_existing(self, bind, *, concurrently: bool = False, schema: str = "public") -> list[str]:
        """
        Refresh only materialized views that already exist.
        """
        refreshed: list[str] = []

        for item in self.plan():
            cls = self._constructs[item.name]
            if materialized_view_exists(bind, cls.__mv_name__, schema=schema):
                cls.refresh_mv(bind, concurrently=concurrently)
                refreshed.append(cls.__mv_name__)

        return refreshed


    def explain(self, bind, *, schema: str = "public", with_counts: bool = True) -> str:
        """
        For each materialized view in the registry, show the SQL definition and whether it exists in the database.
        If with_counts is True, also attempt to query the row count for existing MVs.
        """
        lines = ["<ConstructRegistry explain>"]

        for item in self.plan():
            cls = self._constructs[item.name]

            sql = str(cls.__mv_select__.compile(bind, compile_kwargs={"literal_binds": True}))
            sql_kb = round(len(sql) / 1024, 1)

            exists = materialized_view_exists(bind, cls.__mv_name__, schema=schema)

            row_info = ""
            if with_counts and exists:
                try:
                    count = bind.execute(
                        sa.text(f"SELECT count(*) FROM {schema}.{cls.__mv_name__}")
                    ).scalar()
                    row_info = f", rows={count:,}"
                except Exception as e:
                    row_info = f", rows=<?> ({e.__class__.__name__})"

            lines.append(
                f"  - {cls.__mv_name__:25s}  sql≈{sql_kb:6.1f} KB, exists={exists}{row_info}"
            )

        return "\n".join(lines)

    def validate(self, bind, *, schema: str = "public") -> str:
        """
        Check to see if each materialized view exists, and if so whether its schema matches 
        the mapper definition.
        """
        lines = ["<ConstructRegistry validate>"]

        for item in self.plan():
            cls = self._constructs[item.name]
            name = cls.__mv_name__

            exists = materialized_view_exists(bind, name, schema=schema)
            if not exists:
                lines.append(f"  ✗ {name}: MV does not exist")
                continue

            cols = bind.execute(sa.text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :name
                ORDER BY ordinal_position
            """), {"schema": schema, "name": name}).scalars().all()

            orm_cols = {
                k for k, v in vars(cls).items()
                if hasattr(v, "property") or hasattr(v, "type")
            }

            missing = sorted(set(orm_cols) - set(cols))
            extra = sorted(set(cols) - set(orm_cols))

            if not missing and not extra:
                lines.append(f"  ✓ {name}: schema OK")
            else:
                lines.append(f"  ⚠ {name}: schema mismatch")
                if missing:
                    lines.append(f"      - missing in MV: {missing}")
                if extra:
                    lines.append(f"      - extra in MV:   {extra}")

        return "\n".join(lines)


    def build_plan_json(self) -> dict:
        """
        Dumps plan information as JSON-serializable dict, e.g. for API responses or logging.
        """
        items = self.plan()

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "constructs": [
                {
                    "name": item.name,
                    "kind": item.kind,
                    "deps": list(item.deps),
                }
                for item in items
            ],
        }

    def status(self, bind, *, schema: str = "public") -> str:
        """
        Which each MV exists or not, in dependency order.
        """
        lines = ["<ConstructRegistry status>"]
        for item in self.plan():
            name = item.name
            exists = materialized_view_exists(bind, name, schema=schema)
            flag = "✓" if exists else "✗"
            lines.append(f"  {flag} {name}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        try:
            items = self.plan()
        except Exception:
            # fallback if deps are broken / incomplete during import time
            names = sorted(self._constructs.keys())
            body = ", ".join(names)
            return f"<ConstructRegistry [{body}]>"

        body = ", ".join(item.name for item in items)
        return f"<ConstructRegistry [{body}]>"
    

    def describe(self, full: bool = False) -> str:
        """
        Render a text description of the registry dependency graph.
        """
        try:
            items = self.plan()
        except Exception:
            return f"<ConstructRegistry (invalid dependency graph) n={len(self._constructs)}>"

        lines = ["<ConstructRegistry>"]
        for item in items:
            deps = ", ".join(item.deps) if item.deps else "—"
            lines.append(f"  - {item.name:25s}  kind={item.kind:18s} deps=[{deps}]")

            if full:
                cls = self._constructs[item.name]
                sql = str(cls.__mv_select__)
                lines.append(f"      sql_length={len(sql):,} chars")

        return "\n".join(lines)
    

    def ascii_dag(self) -> str:
        """
        Render a simple ASCII dependency graph of the construct registry.
        """

        try:
            items = self.plan()
        except Exception as e:
            return f"<ConstructRegistry DAG unavailable: {e}>"

        # adjacency: parent -> children
        children: dict[str, list[str]] = {name: [] for name in self._constructs}
        for name, cls in self._constructs.items():
            for dep in cls.__deps__:
                if dep in children:
                    children[dep].append(name)

        roots = [name for name, cls in self._constructs.items() if not cls.__deps__]

        lines: list[str] = []

        def walk(node: str, prefix: str = "", is_last: bool = True):
            connector = "└─ " if is_last else "├─ "
            lines.append(prefix + connector + node)

            kids = children.get(node, [])
            for i, child in enumerate(kids):
                last = i == len(kids) - 1
                next_prefix = prefix + ("   " if is_last else "│  ")
                walk(child, next_prefix, last)

        for i, root in enumerate(roots):
            last_root = i == len(roots) - 1
            walk(root, "", last_root)

        return "\n".join(lines)
