from __future__ import annotations

import importlib
import sys

import pytest

from omop_constructs import get_complete_construct_registry
from omop_constructs.core.registry import materialized_view_exists


def _clear_construct_import_state() -> None:
    constructs = importlib.import_module("omop_constructs.core.constructs")
    constructs._CONSTRUCTS.clear()

    for module_name in list(sys.modules):
        if (
            module_name.startswith("omop_constructs.alchemy")
            or module_name.startswith("omop_constructs.semantics")
            or module_name in {"omop_constructs.bootstrap", "omop_constructs.bootstrap_modules"}
        ):
            sys.modules.pop(module_name, None)


@pytest.mark.postgres
def test_full_construct_registry_compile_check_on_postgres(pg_bootstrapped_engine):
    _clear_construct_import_state()
    registry = get_complete_construct_registry()

    report = registry.compile_check()
    assert "primary_diagnosis_condition_mv" in report

    created = []
    try:
        created = registry.create_missing(pg_bootstrapped_engine, with_data=False)
        assert "primary_diagnosis_condition_mv" in created
        assert materialized_view_exists(
            pg_bootstrapped_engine,
            "primary_diagnosis_condition_mv",
        )
    finally:
        registry.drop_all(pg_bootstrapped_engine, cascade=True)
