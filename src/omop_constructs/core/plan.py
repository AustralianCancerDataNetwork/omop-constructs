from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict, deque

from .errors import DependencyCycleError

@dataclass(frozen=True)
class ConstructNode:
    name: str
    deps: tuple[str, ...] = ()
    kind: str = "materialized_view"

def topo_sort(nodes: list[ConstructNode]) -> list[ConstructNode]:
    by_name = {n.name: n for n in nodes}
    indeg = {n.name: 0 for n in nodes}
    adj = defaultdict(list)

    for n in nodes:
        for d in n.deps:
            if d not in by_name:
                # allow external deps; they won't be in the plan
                continue
            adj[d].append(n.name)
            indeg[n.name] += 1

    q = deque([name for name, deg in indeg.items() if deg == 0])
    out = []

    while q:
        name = q.popleft()
        out.append(by_name[name])
        for nxt in adj.get(name, []):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.append(nxt)

    if len(out) != len(nodes):
        # cycle: find remaining
        remaining = [n for n, deg in indeg.items() if deg > 0]
        raise DependencyCycleError(f"Dependency cycle detected among: {remaining}")

    return out
