from __future__ import annotations

from pathlib import Path

from oa_configurator import CDMDatabaseConfig, ConnectionConfig, StackConfig

from omop_constructs.cli import main
from omop_constructs.config import OmopConstructsConfig, create_cdm_engine


def test_config_defaults_to_the_shared_cdm_database_without_a_tool_section() -> None:
    """A package with no ``[tools.omop_constructs]`` section still resolves the CDM.

    ``cdm_db`` defaults to the entry of the same name, which is how this package
    shares omop-alchemy's CDM by naming convention rather than by import.
    """
    stack = StackConfig.for_session(
        connections={
            "cdm_db": ConnectionConfig(dialect="sqlite", database_name=":memory:")
        },
        databases={
            "cdm_db": CDMDatabaseConfig(connection="cdm_db", schema_name="omop")
        },
    )

    assert "omop_constructs" not in stack.tools

    engine = create_cdm_engine(stack)

    assert engine.url.render_as_string(hide_password=False) == "sqlite:///:memory:"


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


def test_create_cdm_engine_resolves_configured_database_to_expected_url() -> None:
    """Pin the *outcome* of CDM resolution, not the mechanism.

    The oa-configurator 1.x migration deleted the whole resource API and the
    per-tool default-resource cascade, so the tests that asserted on those were
    rewritten or dropped with it. The engine URL is the part that had to survive:
    the fixture below was rebuilt on the 1.x models while this assertion stayed
    byte-identical. A different URL would have meant the migration repointed the CDM.
    """
    stack = StackConfig.for_session(
        connections={"cdm": ConnectionConfig(dialect="sqlite", database_name=":memory:")},
        databases={"cdm_db": CDMDatabaseConfig(connection="cdm", schema_name="omop")},
    )

    engine = create_cdm_engine(stack)

    assert engine.url.render_as_string(hide_password=False) == "sqlite:///:memory:"
    assert engine.dialect.name == "sqlite"


def test_create_cdm_engine_falls_back_when_config_file_is_schema_invalid(
    monkeypatch, tmp_path: Path
) -> None:
    """An unreadable config file must not deprive callers of the env fallback.

    ``load_stack_config`` raises ``FileNotFoundError`` only when the file is
    absent. A file that exists but does not match the current schema raises a
    pydantic ``ValidationError`` instead, which is ordinary mid-migration when the
    file on disk and the installed oa-configurator disagree about the layout.
    Before this was handled, that state defeated ``ENGINE_CDM`` entirely.
    """
    invalid = tmp_path / "config.toml"
    invalid.write_text('[databases.cdm_db]\nbogus_field = "nope"\n', encoding="utf-8")
    invalid.chmod(0o600)

    monkeypatch.setattr("oa_configurator.loader.CONFIG_PATH", invalid)
    monkeypatch.setenv("ENGINE_CDM", "sqlite:///fallback.db")

    engine = create_cdm_engine()

    assert engine.url.render_as_string() == "sqlite:///fallback.db"
