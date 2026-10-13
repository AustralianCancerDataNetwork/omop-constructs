from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from omop_alchemy.toolkit.core.concepts import ConceptResolverRegistry

from .runtime_lookups import get_runtime_resolvers, get_registry_engine


class _LazyResolverRegistry:
    """Create the database-backed resolver registry only when it is used."""

    def __init__(self) -> None:
        self._registry: ConceptResolverRegistry | None = None

    def _get_registry(self) -> ConceptResolverRegistry:
        if self._registry is None:
            self._registry = get_runtime_resolvers(get_registry_engine())
        return self._registry

    def __getitem__(self, name: str) -> Any:
        return self._get_registry()[name]

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get_registry(), name)


registry = _LazyResolverRegistry()


def __getattr__(name: str) -> sa.Engine:
    if name == "registry_engine":
        return get_registry_engine()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "get_runtime_resolvers",
    "get_registry_engine",
    "registry",
]
