from __future__ import annotations

import os
from typing import ClassVar

import sqlalchemy as sa
from oa_configurator import (
    ConfigurationError,
    PackageConfigBase,
    Resolver,
    StackConfig,
    load_stack_config,
)


class OmopConstructsConfig(PackageConfigBase):
    """Package-level configuration surface for omop-constructs."""

    tool_name: ClassVar[str] = "omop_constructs"
    required_resources: ClassVar[tuple[str, ...]] = ("cdm_db",)
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ("orm_loader", "omop_alchemy")


def resolve_cdm_resource_name(stack: StackConfig) -> str:
    """Return the CDM resource name used for resolver-backed construct imports.

    The package prefers its own configured default resource when present. When
    ``omop-constructs`` has not been configured explicitly, it falls back to
    ``omop_alchemy``'s default resource so the analytical layer follows the same
    CDM selection as the base OMOP models by default.
    """
    available: set[str] = set(stack.resource_names())
    if stack.active_profile and stack.active_profile in stack.profiles:
        available |= set(stack.profiles[stack.active_profile].resources)

    candidates: list[str] = []
    for tool_name in (OmopConstructsConfig.tool_name, "omop_alchemy"):
        tool = stack.tools.get(tool_name)
        if tool is not None and tool.default_resource:
            candidates.append(tool.default_resource)

    candidates.extend(OmopConstructsConfig.required_resources)

    for resource_name in candidates:
        resolved_name = stack.resource_aliases.get(resource_name, resource_name)
        if resolved_name in available:
            return resource_name

    alias_hint = (
        "\nTip: if you named your resource differently, add:\n"
        '  [resource_aliases]\n  cdm_db = "your-resource-name"'
    )
    raise ConfigurationError(
        "OmopConstructsConfig requires a resolvable CDM resource. "
        f"Tried: {candidates}\n"
        f"Available: {sorted(available) or '(none)'}\n"
        "Run 'omop-config configure omop_constructs' to set up this package, "
        "or configure 'omop_alchemy' first if it owns the shared CDM resource."
        + alias_hint
    )


def create_cdm_engine(stack: StackConfig | None = None) -> sa.Engine:
    """Create the SQLAlchemy engine used by resolver-backed construct imports.

    Runtime usage prefers the shared ``oa-configurator`` stack configuration.
    For test and scratch-database workflows, ``ENGINE_CDM`` or ``ENGINE`` can
    supply a direct SQLAlchemy URL when no config file is present.
    """
    if stack is None:
        try:
            stack = load_stack_config()
        except FileNotFoundError:
            engine_url = os.getenv("ENGINE_CDM") or os.getenv("ENGINE")
            if engine_url:
                return sa.create_engine(engine_url, future=True)
            raise

    resource_name = resolve_cdm_resource_name(stack)
    return Resolver(stack).resolve_resource(resource_name).create_engine()


__all__ = [
    "OmopConstructsConfig",
    "create_cdm_engine",
    "resolve_cdm_resource_name",
]
