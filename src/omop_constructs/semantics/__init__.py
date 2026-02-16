from .runtime_lookups import get_runtime_resolvers, get_registry_engine

registry_engine = get_registry_engine()
registry = get_runtime_resolvers(registry_engine)

__all__ = [
    "get_runtime_resolvers",
    "get_registry_engine",
    "registry",
    "registry_engine",
]