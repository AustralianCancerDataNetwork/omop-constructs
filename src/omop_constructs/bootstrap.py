"""
Helpers for loading the complete construct registry.

This module exists to give callers a single "load everything" entrypoint
without relying on the broad re-export behavior of package ``__init__`` files.
The bootstrap path imports construct-defining modules directly from the
declarative manifest in :mod:`omop_constructs.bootstrap_modules`, which keeps
registry assembly separate from the public import ergonomics of the alchemy
packages.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

from .bootstrap_modules import CDM_CONSTRUCT_MODULES

if TYPE_CHECKING:
    from .core.registry import ConstructRegistry

def _load_construct_modules(module_names: tuple[str, ...]) -> None:
    """
    Import each construct module in order.

    Python's normal module caching makes repeated calls effectively idempotent
    at the import layer, so this helper intentionally avoids adding a second
    caching mechanism here.
    """
    for module_name in module_names:
        import_module(module_name)

def load_construct_families() -> None:
    """
    Import every construct module required for the full CDM registry.

    This performs registration as a side effect because construct classes attach
    themselves to the global registry when their modules are imported.
    """
    _load_construct_modules(CDM_CONSTRUCT_MODULES)

def get_complete_construct_registry() -> ConstructRegistry:
    """
    Return a registry containing the full shipped construct surface.

    Unlike ``omop_constructs.core.get_construct_registry()``, this helper first
    ensures that the full construct manifest has been imported in the current
    process.
    """
    load_construct_families()
    constructs_module = import_module("omop_constructs.core.constructs")
    return constructs_module.get_construct_registry()

def list_cdm_matview_names() -> list[str]:
    """
    Return every registered materialized view name in dependency order.

    This is a convenience wrapper for callers that only need the planned MV
    names and do not need direct access to the registry instance.
    """
    return [item.name for item in get_complete_construct_registry().plan()]
