
from typing import Type, Dict, TypeVar
from .registry import ConstructRegistry
from ..typing import SupportsMaterializedView

T = TypeVar("T", bound=Type[SupportsMaterializedView])

_CONSTRUCTS: Dict[str, Type[SupportsMaterializedView]] = {}

def register_construct(cls: T) -> T:
    """
    Decorator to register a materialized view / construct class.
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
    return ConstructRegistry(_CONSTRUCTS.values())