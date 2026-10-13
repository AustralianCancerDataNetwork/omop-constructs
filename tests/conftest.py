from __future__ import annotations

from pathlib import Path
import sys

import pytest
import sqlalchemy as sa
import oa_configurator.loader as oa_loader
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, StackConfig, save_stack_config
from oa_configurator.testing import isolated_test_database
from omop_alchemy.config import OmopAlchemyConfig
from orm_loader.helpers.bootstrap import create_db


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SEMANTICS_SRC = ROOT.parent / "omop-semantics" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

if str(SEMANTICS_SRC) not in sys.path:
    sys.path.insert(0, str(SEMANTICS_SRC))


@pytest.fixture
def pg_bootstrapped_engine(monkeypatch, request, tmp_path: Path):
    """Use the configured test-only PostgreSQL database and its isolated schema."""
    with isolated_test_database(
        OmopAlchemyConfig,
        "test_cdm_db_pg",
        request=request,
    ) as database:
        engine = database.committing_engine
        create_db(database.resolved, bindable=engine)

        url = sa.engine.make_url(engine.url.render_as_string(hide_password=False))
        connection = ConnectionConfig(
            dialect=url.drivername,
            host=url.host,
            port=url.port,
            user=url.username,
            password=url.password,
            database_name=url.database,
            test_only=True,
        )
        cdm = CDMDatabaseConfig(
            connection="cdm_db",
            cdm_schema=database.resolved.cdm_schema,
            vocab_schema=database.resolved.vocab_schema,
            results_schema=database.resolved.results_schema,
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
        yield engine
