"""
Small helpers for lazy package re-exports.

The alchemy family ``__init__`` modules use this helper so they can remain
pleasant to import from while avoiding eager import fanout during package
initialization.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


def load_export(name: str, export_map: dict[str, str]) -> Any:
    """
    Resolve an exported attribute on first access.

    Parameters
    ----------
    name:
        The attribute requested from a package ``__init__`` module.
    export_map:
        Mapping from exported attribute name to the leaf module that defines it.
    """
    try:
        module_name = export_map[name]
    except KeyError as exc:
        raise AttributeError(name) from exc

    module = import_module(module_name)
    return getattr(module, name)
