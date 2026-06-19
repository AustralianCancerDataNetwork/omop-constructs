import os
from pathlib import Path
import sys
import tempfile
import time
from uuid import uuid4

import pytest
import sqlalchemy as sa
import oa_configurator.loader as oa_loader
from oa_configurator import (
    DatabaseConfig,
    Resolver,
    ResourceConfig,
    StackConfig,
    load_stack_config,
    save_stack_config,
)
from omop_alchemy.maintenance import create_missing_tables
from omop_alchemy.maintenance._cli_utils import ensure_schema
from omop_alchemy.maintenance.tables import (
    collect_maintenance_tables,
    schema_adjusted_metadata,
)
from omop_constructs.config import resolve_cdm_resource_name


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SEMANTICS_SRC = ROOT.parent / "omop-semantics" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

if str(SEMANTICS_SRC) not in sys.path:
    sys.path.insert(0, str(SEMANTICS_SRC))


def _build_database_config(url: sa.engine.URL) -> DatabaseConfig:
    return DatabaseConfig(
        dialect=url.drivername,
        host=url.host,
        port=url.port,
        user=url.username,
        password=url.password,
        database_name=url.database,
    )


def _load_effective_cdm_resource() -> ResourceConfig | None:
    try:
        stack = load_stack_config()
    except FileNotFoundError:
        return None

    resolved = Resolver(stack).resolve_resource(resolve_cdm_resource_name(stack))
    return ResourceConfig(
        database="cdm_db",
        vocab_database="cdm_db",
        cdm_schema=resolved.cdm_schema,
        vocab_schema=resolved.vocab_schema,
        results_schema=resolved.results_schema,
    )


def _build_scratch_stack(
    scratch_url: sa.engine.URL,
    *,
    resource_config: ResourceConfig,
) -> StackConfig:
    return StackConfig.for_session(
        databases={"cdm_db": _build_database_config(scratch_url)},
        resources={"cdm_db": resource_config},
    )


def _bootstrap_scratch_cdm(
    engine: sa.Engine,
    *,
    resource_config: ResourceConfig,
) -> None:
    cdm_schema = resource_config.cdm_schema
    vocab_schema = resource_config.vocab_schema or cdm_schema
    results_schema = resource_config.results_schema

    if results_schema:
        ensure_schema(engine, results_schema)

    if vocab_schema == cdm_schema:
        create_missing_tables(
            engine,
            db_schema=cdm_schema,
            vocabulary_included=True,
        )
        return

    ensure_schema(engine, cdm_schema)
    ensure_schema(engine, vocab_schema)

    cdm_tables = []
    vocab_tables = []
    for maintenance_table in collect_maintenance_tables():
        if maintenance_table.is_vocabulary:
            vocab_tables.append(maintenance_table)
        else:
            cdm_tables.append(maintenance_table)

    with engine.begin() as connection:
        for tables, db_schema in (
            (cdm_tables, cdm_schema),
            (vocab_tables, vocab_schema),
        ):
            metadata, adjusted_tables = schema_adjusted_metadata(
                tables,
                db_schema=db_schema,
            )
            metadata.create_all(
                bind=connection,
                tables=[adjusted_tables[table.table_name] for table in tables],
                checkfirst=True,
            )


def _wait_for_engine(engine: sa.Engine, *, attempts: int = 20) -> None:
    for attempt in range(attempts):
        try:
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            return
        except Exception:
            if attempt == attempts - 1:
                engine.dispose()
                pytest.fail(
                    "PostgreSQL database not available after 20 attempts. "
                    "Set ENGINE_CDM or ENGINE to a reachable PostgreSQL database."
                )
            time.sleep(1)


def _scratch_database_name(source_name: str | None) -> str:
    source = (source_name or "omop").lower()
    safe = "".join(ch if ch.isalnum() else "_" for ch in source).strip("_") or "omop"
    prefix = f"{safe[:32]}_constructs_test"
    suffix = uuid4().hex[:8]
    return f"{prefix}_{suffix}"[:63]


def _database_exists(admin_engine: sa.Engine, database_name: str) -> bool:
    with admin_engine.connect() as conn:
        return bool(
            conn.execute(
                sa.text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_database
                        WHERE datname = :database_name
                    )
                    """
                ),
                {"database_name": database_name},
            ).scalar()
        )


def _create_scratch_database(
    admin_engine: sa.Engine,
    *,
    source_name: str | None,
) -> str:
    database_name = _scratch_database_name(source_name)
    if _database_exists(admin_engine, database_name):
        pytest.fail(
            "Refusing to run PostgreSQL integration test because the scratch "
            f"database '{database_name}' already exists."
        )

    with admin_engine.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{database_name}"'))
    return database_name


def _drop_database_if_exists(admin_engine: sa.Engine, database_name: str) -> None:
    with admin_engine.connect() as conn:
        conn.execute(
            sa.text(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = :database_name
                  AND pid <> pg_backend_pid()
                """
            ),
            {"database_name": database_name},
        )
        conn.execute(
            sa.text(f'DROP DATABASE IF EXISTS "{database_name}"')
        )


@pytest.fixture(scope="session")
def pg_engine():
    """
    Session-scoped engine connecting to a PostgreSQL database for opt-in tests.

    Resolution order:
    1. configured ``cdm_db`` resource from ``oa-configurator``
    2. ``ENGINE_CDM``
    3. ``ENGINE``
    """
    resource_config = _load_effective_cdm_resource() or ResourceConfig(
        database="cdm_db",
        vocab_database="cdm_db",
        cdm_schema="public",
        vocab_schema="public",
    )

    engine_url = None
    configured_resource = None
    try:
        stack = load_stack_config()
    except FileNotFoundError:
        stack = None

    if stack is not None:
        configured_resource = Resolver(stack).resolve_resource(
            resolve_cdm_resource_name(stack)
        )
        if configured_resource.database.url.startswith("postgresql"):
            engine_url = configured_resource.database.url

    if engine_url is None:
        engine_url = os.getenv("ENGINE_CDM") or os.getenv("ENGINE")

    if not engine_url:
        pytest.skip(
            "No PostgreSQL source configured. Configure a PostgreSQL cdm_db "
            "resource or set ENGINE_CDM / ENGINE."
        )

    engine = sa.create_engine(engine_url, future=True)
    if engine.url.drivername != "postgresql+psycopg" and not engine.url.drivername.startswith(
        "postgresql"
    ):
        engine.dispose()
        pytest.skip(
            "The configured PostgreSQL integration source must use a PostgreSQL "
            f"SQLAlchemy URL, got {engine.url.drivername!r}."
        )
    _wait_for_engine(engine)
    try:
        yield engine, resource_config
    finally:
        engine.dispose()


@pytest.fixture
def pg_bootstrapped_engine(pg_engine):
    """
    Function-scoped PostgreSQL engine bound to a disposable scratch database.

    The configured PostgreSQL source is treated only as a connection source.
    The fixture creates a separate temporary database, bootstraps OMOP tables
    into the configured CDM and vocabulary schemas, points resolver-backed
    imports at that scratch database via a temporary ``oa-configurator`` stack
    config, and drops the scratch database on teardown.
    """
    source_engine, resource_config = pg_engine
    source_url = sa.engine.make_url(
        source_engine.url.render_as_string(hide_password=False)
    )

    admin_engine = sa.create_engine(
        source_url,
        future=True,
        isolation_level="AUTOCOMMIT",
    )
    temp_config_dir = tempfile.TemporaryDirectory(prefix="omop-constructs-pg-")
    temp_config_path = Path(temp_config_dir.name) / "config.toml"
    original_engine = os.environ.get("ENGINE")
    original_engine_cdm = os.environ.get("ENGINE_CDM")
    original_oa_config_path = os.environ.get("OA_CONFIG_PATH")
    original_loader_config_path = oa_loader.CONFIG_PATH
    scratch_engine = None
    scratch_name = None

    try:
        scratch_name = _create_scratch_database(
            admin_engine,
            source_name=source_url.database,
        )
        scratch_url = source_url.set(database=scratch_name)

        scratch_engine = sa.create_engine(
            scratch_url.render_as_string(hide_password=False),
            future=True,
        )
        _wait_for_engine(scratch_engine)

        _bootstrap_scratch_cdm(
            scratch_engine,
            resource_config=resource_config,
        )

        scratch_stack = _build_scratch_stack(
            scratch_url,
            resource_config=resource_config,
        )
        save_stack_config(scratch_stack, temp_config_path)

        scratch_url_str = scratch_url.render_as_string(hide_password=False)
        oa_loader.CONFIG_PATH = temp_config_path
        os.environ["OA_CONFIG_PATH"] = str(temp_config_path)
        os.environ["ENGINE_CDM"] = scratch_url_str
        os.environ["ENGINE"] = scratch_url_str
        yield scratch_engine
    finally:
        try:
            if scratch_engine is not None:
                scratch_engine.dispose()
            if scratch_name is not None:
                _drop_database_if_exists(admin_engine, scratch_name)
        finally:
            admin_engine.dispose()
            temp_config_dir.cleanup()
            oa_loader.CONFIG_PATH = original_loader_config_path
            if original_engine is None:
                os.environ.pop("ENGINE", None)
            else:
                os.environ["ENGINE"] = original_engine
            if original_engine_cdm is None:
                os.environ.pop("ENGINE_CDM", None)
            else:
                os.environ["ENGINE_CDM"] = original_engine_cdm
            if original_oa_config_path is None:
                os.environ.pop("OA_CONFIG_PATH", None)
            else:
                os.environ["OA_CONFIG_PATH"] = original_oa_config_path
