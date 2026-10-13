from __future__ import annotations

import importlib
import sys

import pytest
from omop_constructs.core.orm_base import ConstructORMBase as Base
from orm_loader.helpers.bootstrap import create_db

from omop_constructs import get_complete_construct_registry
from omop_constructs.core.registry import materialized_view_exists


def _clear_construct_import_state() -> None:
    constructs = importlib.import_module("omop_constructs.core.constructs")
    registered = list(constructs._CONSTRUCTS.values())
    constructs._CONSTRUCTS.clear()

    for cls in registered:
        table = getattr(cls, "__table__", None)
        if table is not None and table.key in Base.metadata.tables:
            Base.metadata.remove(Base.metadata.tables[table.key])

    for module_name in list(sys.modules):
        if (
            module_name.startswith(
                ("omop_constructs.alchemy", "omop_constructs.semantics")
            )
            or module_name in {"omop_constructs.bootstrap", "omop_constructs.bootstrap_modules"}
        ):
            sys.modules.pop(module_name, None)


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_full_construct_registry_compile_check_on_postgres(pg_db):
    _clear_construct_import_state()
    registry = get_complete_construct_registry()

    report = registry.compile_check()
    assert "primary_diagnosis_condition_mv" in report

    with pg_db.engine.connect() as conn:
        created = []
        try:
            created = registry.create_missing(
                conn, with_data=False, schema=pg_db.resolved.cdm_schema
            )
            assert "primary_diagnosis_condition_mv" in created
            assert materialized_view_exists(
                conn,
                "primary_diagnosis_condition_mv",
                schema=pg_db.resolved.cdm_schema,
            )
        finally:
            registry.drop_all(conn, cascade=True)


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_orm_loader_can_bootstrap_after_construct_import(pg_db):
    get_complete_construct_registry()
    create_db(pg_db.resolved, bindable=pg_db.engine)
