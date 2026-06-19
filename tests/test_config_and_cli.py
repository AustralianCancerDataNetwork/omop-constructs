from __future__ import annotations

from pathlib import Path

from oa_configurator import DatabaseConfig, ResourceConfig, StackConfig, ToolConfig

from omop_constructs.cli import main
from omop_constructs.config import (
    OmopConstructsConfig,
    create_cdm_engine,
    resolve_cdm_resource_name,
)


def test_config_uses_shared_cdm_resource_without_tool_section() -> None:
    stack = StackConfig(
        databases={
            "cdm": DatabaseConfig(
                dialect="sqlite",
                database_name=":memory:",
            )
        },
        resources={
            "cdm_db": ResourceConfig(
                database="cdm",
                cdm_schema="omop",
            )
        },
    )

    config = OmopConstructsConfig.from_stack(stack)

    assert config.model_dump() == {}


def test_cli_verbose_flag_configures_logging(monkeypatch, tmp_path: Path) -> None:
    calls: list[int] = []
    output = tmp_path / "registry.csv"

    def fake_configure_logging(*, verbosity: int = 0, console=None) -> None:
        calls.append(verbosity)

    def fake_write_registry_schema_snapshot(path: Path) -> Path:
        path.write_text("construct_name\n", encoding="utf-8")
        return path

    monkeypatch.setattr(OmopConstructsConfig, "configure_logging", fake_configure_logging)
    monkeypatch.setattr("omop_constructs.cli.write_registry_schema_snapshot", fake_write_registry_schema_snapshot)

    status = main(["-v", "schema-snapshot", str(output)])

    assert status == 0
    assert calls == [1]
    assert output.exists()


def test_resource_resolution_falls_back_to_omop_alchemy_default_resource() -> None:
    stack = StackConfig(
        databases={
            "cdm": DatabaseConfig(
                dialect="sqlite",
                database_name=":memory:",
            )
        },
        resources={
            "cdm_db_alt": ResourceConfig(
                database="cdm",
                cdm_schema="omop",
            )
        },
        tools={
            "omop_alchemy": ToolConfig(
                default_resource="cdm_db_alt",
                extra={},
            )
        },
    )

    assert resolve_cdm_resource_name(stack) == "cdm_db_alt"


def test_resource_resolution_prefers_omop_constructs_default_resource() -> None:
    stack = StackConfig(
        databases={
            "cdm": DatabaseConfig(
                dialect="sqlite",
                database_name=":memory:",
            )
        },
        resources={
            "cdm_db": ResourceConfig(
                database="cdm",
                cdm_schema="omop",
            ),
            "cdm_reporting": ResourceConfig(
                database="cdm",
                cdm_schema="reporting",
            ),
        },
        tools={
            "omop_alchemy": ToolConfig(
                default_resource="cdm_db",
                extra={},
            ),
            "omop_constructs": ToolConfig(
                default_resource="cdm_reporting",
                extra={},
            ),
        },
    )

    assert resolve_cdm_resource_name(stack) == "cdm_reporting"


def test_create_cdm_engine_falls_back_to_environment(monkeypatch) -> None:
    sentinel = object()
    calls: list[tuple[str, bool]] = []

    def fake_load_stack_config() -> StackConfig:
        raise FileNotFoundError("missing config")

    def fake_create_engine(url: str, *, future: bool) -> object:
        calls.append((url, future))
        return sentinel

    monkeypatch.setattr("omop_constructs.config.load_stack_config", fake_load_stack_config)
    monkeypatch.setattr("omop_constructs.config.sa.create_engine", fake_create_engine)
    monkeypatch.setenv("ENGINE_CDM", "sqlite:///:memory:")

    assert create_cdm_engine() is sentinel
    assert calls == [("sqlite:///:memory:", True)]
