import os
from pathlib import Path
import sys
import time
from uuid import uuid4

import pytest
import sqlalchemy as sa
from omop_alchemy.maintenance import create_missing_tables


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SEMANTICS_SRC = ROOT.parent / "omop-semantics" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

if str(SEMANTICS_SRC) not in sys.path:
    sys.path.insert(0, str(SEMANTICS_SRC))


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
    1. ``ENGINE_CDM``
    2. ``ENGINE``
    """
    engine_url = os.getenv("ENGINE_CDM") or os.getenv("ENGINE")
    if not engine_url:
        pytest.skip("No PostgreSQL engine configured. Set ENGINE_CDM or ENGINE.")

    engine = sa.create_engine(engine_url, future=True)
    _wait_for_engine(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def pg_bootstrapped_engine(pg_engine):
    """
    Function-scoped PostgreSQL engine bound to a disposable scratch database.

    The configured database in ``ENGINE_CDM`` / ``ENGINE`` is treated only as a
    connection source. The fixture creates a separate temporary database,
    bootstraps OMOP tables there, points ``ENGINE`` at that scratch database for
    import-time resolver setup, and drops the scratch database on teardown.
    """
    source_url = sa.engine.make_url(
        pg_engine.url.render_as_string(hide_password=False)
    )

    admin_engine = sa.create_engine(
        source_url,
        future=True,
        isolation_level="AUTOCOMMIT",
    )
    original_engine = os.environ.get("ENGINE")
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

        create_missing_tables(scratch_engine, vocabulary_included=True)
        os.environ["ENGINE"] = scratch_url.render_as_string(hide_password=False)
        yield scratch_engine
    finally:
        try:
            if scratch_engine is not None:
                scratch_engine.dispose()
            if scratch_name is not None:
                _drop_database_if_exists(admin_engine, scratch_name)
        finally:
            admin_engine.dispose()
            if original_engine is None:
                os.environ.pop("ENGINE", None)
            else:
                os.environ["ENGINE"] = original_engine
