from __future__ import annotations

from pathlib import Path
import sys

import pytest
import sqlalchemy as sa
import oa_configurator.loader as oa_loader
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, StackConfig, save_stack_config
from oa_configurator.testing import isolated_test_database, scoped_test_schema
from omop_alchemy.config import OmopAlchemyConfig
from omop_alchemy.maintenance.tables import select_omop_tables
from orm_loader.helpers import create_tables


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SEMANTICS_SRC = ROOT.parent / "omop-semantics" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

if str(SEMANTICS_SRC) not in sys.path:
    sys.path.insert(0, str(SEMANTICS_SRC))


@pytest.fixture
def pg_db(monkeypatch, request, tmp_path: Path):
    """Use the configured test-only PostgreSQL database in a disposable schema."""
    with isolated_test_database(
        OmopAlchemyConfig,
        "test_cdm_db_pg",
        request=request,
    ) as database:
        with scoped_test_schema(database.resolved, prefix="omop_constructs") as scoped:
            quoted_schema = scoped.engine.dialect.identifier_preparer.quote(
                scoped.resolved.cdm_schema
            )

            def set_test_search_path(
                dbapi_connection, _connection_record, _connection_proxy
            ):
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute(f"SET search_path TO {quoted_schema}")
                finally:
                    cursor.close()

            sa.event.listen(scoped.engine, "checkout", set_test_search_path)
            cdm_tables = [
                target.table
                for target in select_omop_tables(vocabulary_included=True)
            ]
            with scoped.engine.begin() as connection:
                create_tables(connection, cdm_tables, resolved=scoped.resolved)

            url = sa.engine.make_url(
                scoped.engine.url.render_as_string(hide_password=False)
            )
            # Runtime imports resolve through this schema-scoped CDM reference.
            # Mark it non-test because isolated_test_database has already
            # verified the test-only source connection.
            connection = ConnectionConfig(
                dialect=url.drivername,
                host=url.host,
                port=url.port,
                user=url.username,
                password=url.password,
                database_name=url.database,
                test_only=False,
            )
            cdm = CDMDatabaseConfig(
                connection="cdm_db",
                cdm_schema=scoped.resolved.cdm_schema,
                vocab_schema=scoped.resolved.vocab_schema,
                results_schema=scoped.resolved.results_schema,
            )
            stack = StackConfig.for_session(
                connections={"cdm_db": connection},
                databases={"cdm_db": cdm},
            )
            config_path = tmp_path / "config.toml"
            save_stack_config(stack, config_path)
            monkeypatch.setattr(oa_loader, "CONFIG_PATH", config_path)
            monkeypatch.setenv("OA_CONFIG_PATH", str(config_path))
            monkeypatch.setenv("ENGINE_CDM", url.render_as_string(hide_password=False))
            monkeypatch.setenv("ENGINE", url.render_as_string(hide_password=False))
            yield scoped


@pytest.fixture
def pg_bootstrapped_engine(pg_db):
    return pg_db.engine
