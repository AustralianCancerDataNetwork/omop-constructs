from __future__ import annotations

import os
from typing import Annotated, ClassVar

import sqlalchemy as sa
from oa_configurator import (
    CDMDatabaseConfig,
    PackageConfigBase,
    RefTo,
    Resolver,
    StackConfig,
    load_stack_config,
)


class OmopConstructsConfig(PackageConfigBase):
    """Package-level configuration surface for omop-constructs.

    The ``cdm_db`` field names the ``[databases.*]`` entry holding the CDM.
    Defaulting it to ``"cdm_db"`` is what shares that database with
    ``omop-alchemy``, which declares an identically-named field — the two agree
    by naming convention, deliberately, rather than by either importing the
    other's config class.
    """

    tool_name: ClassVar[str] = "omop_constructs"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ("orm_loader", "omop_alchemy")

    cdm_db: Annotated[str, RefTo(CDMDatabaseConfig)] = "cdm_db"


def create_cdm_engine(stack: StackConfig | None = None) -> sa.Engine:
    """Create the SQLAlchemy engine used by resolver-backed construct imports.

    Runtime usage prefers the shared ``oa-configurator`` stack configuration.
    For test and scratch-database workflows, ``ENGINE_CDM`` or ``ENGINE`` can
    supply a direct SQLAlchemy URL when the configuration cannot be loaded.

    The fallback covers an unreadable config file as well as a missing one.
    ``load_stack_config`` raises ``FileNotFoundError`` when absent, but
    ``ValueError`` for malformed TOML and a pydantic ``ValidationError`` — itself
    a ``ValueError`` — when the file does not match the current schema. That last
    case is ordinary during a stack migration, when the file on disk and the
    installed oa-configurator disagree about the layout, and it should not
    deprive callers of the environment escape hatch.
    """
    if stack is None:
        try:
            stack = load_stack_config()
        except (FileNotFoundError, ValueError):
            engine_url = os.getenv("ENGINE_CDM") or os.getenv("ENGINE")
            if engine_url:
                return sa.create_engine(engine_url, future=True)
            raise

    resolver = Resolver(stack)
    config = resolver.resolve_package_config(OmopConstructsConfig)
    return resolver.resolve_engine(config.cdm_db)


__all__ = [
    "OmopConstructsConfig",
    "create_cdm_engine",
]
