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
    
    def create_all(self, bind, *, with_data: bool = True) -> None:
        for item in self.plan():
            self._constructs[item.name].create_mv(bind, with_data=with_data)

    def refresh_all(self, bind, *, concurrently: bool = False) -> None:
        for item in self.plan():
            self._constructs[item.name].refresh_mv(bind, concurrently=concurrently)

    def drop_all(self, bind, *, cascade: bool = False) -> None:
        # drop reverse order
        for item in reversed(self.plan()):
            self._constructs[item.name].drop_mv(bind, cascade=cascade)
