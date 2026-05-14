
from typing import Type, Dict, TypeVar
from .registry import ConstructRegistry
from ..typing import SupportsMaterializedView

T = TypeVar("T", bound=Type[SupportsMaterializedView])

_CONSTRUCTS: Dict[str, Type[SupportsMaterializedView]] = {}

def register_construct(cls: T) -> T:
    """
    Register a materialized view-backed construct class.

    Registration is import-driven: any module that defines construct classes and
    imports this decorator will add those classes to the in-process registry as
    soon as the module is imported.
    """
    name = getattr(cls, "__mv_name__", None)
    if not name:
        raise ValueError(
            f"{cls.__name__} must define __mv_name__ to be registered as a construct"
        )

    if name in _CONSTRUCTS:
        raise ValueError(f"Duplicate construct registration for '{name}'")

    _CONSTRUCTS[name] = cls
    return cls

def get_construct_registry() -> ConstructRegistry:
    """
    Return a registry view over all construct classes imported so far.

    Notes
    -----
    The returned registry reflects the current Python process. If a construct
    module has not yet been imported, its classes will not appear here.
    """
    return ConstructRegistry(_CONSTRUCTS.values())
