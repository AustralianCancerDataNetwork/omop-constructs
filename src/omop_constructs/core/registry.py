from typing import Iterable, Iterator, Type
from .materialized import MaterializedViewMixin
from .plan import ConstructNode, topo_sort
from ..typing import SupportsMaterializedView
from dataclasses import dataclass

@dataclass(frozen=True)
class ConstructPlanItem:
    name: str
    kind: str
    deps: tuple[str, ...]

class ConstructRegistry:
    """
    Registry of materialized views / constructs.

    Handles creation, refresh, and teardown in a controlled way.
    """
    _constructs: dict[str, Type[SupportsMaterializedView]]

    def __init__(self, constructs: Iterable[type[SupportsMaterializedView]]):
        self._constructs = {c.__mv_name__: c for c in constructs}
        
    def __iter__(self) -> Iterator[type[SupportsMaterializedView]]:
        return iter(self._constructs.values())

    def get(self, name: str) -> type[SupportsMaterializedView]:
        return self._constructs[name]

    def plan(self) -> tuple[ConstructPlanItem, ...]:
        nodes = [
            ConstructNode(name=k, deps=v.__deps__, kind=getattr(v.__construct__, "kind", "materialized_view"))
            for k, v in self._constructs.items()
        ]
        ordered = topo_sort(nodes)
        return tuple(ConstructPlanItem(n.name, n.kind, n.deps) for n in ordered)
    
    def create_all(self, bind, *, with_data: bool = True):
        for item in self.plan():
            cls = self._constructs[item.name]
            cls.create_mv(bind, with_data=with_data)

    def refresh_all(self, bind, *, concurrently: bool = False) -> None:
        for item in self.plan():
            self._constructs[item.name].refresh_mv(bind, concurrently=concurrently)

    def drop_all(self, bind, *, cascade: bool = False) -> None:
        # drop reverse order
        for item in reversed(self.plan()):
            self._constructs[item.name].drop_mv(bind, cascade=cascade)


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
    

    def describe(self) -> str:
        try:
            items = self.plan()
        except Exception:
            return f"<ConstructRegistry (invalid dependency graph) n={len(self._constructs)}>"

        lines = ["<ConstructRegistry>"]
        for item in items:
            deps = ", ".join(item.deps) if item.deps else "—"
            lines.append(f"  - {item.name:25s}  kind={item.kind:18s} deps=[{deps}]")

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