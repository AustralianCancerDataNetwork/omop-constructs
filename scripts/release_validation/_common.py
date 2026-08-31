"""Shared helpers for the transitional release validation scripts.

These scripts are deliberately outside the installed package. They orchestrate
two *different* installed versions of omop-constructs against one CDM, which is
not something the library itself should be able to do, and they are expected to
be deleted once the lifecycle work in OC-3 gives the registry schema-qualified
DDL and a supported comparison entry point.

Two rules shape everything here:

- Non-clinical metadata (row counts, checksums, duplicate counts) and clinical
  results (row-level differences, person identifiers) are written to separate
  subdirectories, so an operator can review one without moving the other.
- Clinical output is refused inside the repository working tree. A misdirected
  ``--output-dir`` is the most likely way patient data ends up somewhere it
  should not be, and a check is cheaper than a retraction.
"""

from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import sqlalchemy as sa

#: PostgreSQL identifiers we are willing to interpolate into DDL. Everything
#: here is operator-supplied on the command line and reaches SQL as text rather
#: than as a bind parameter — schema names cannot be parameterised — so the
#: pattern is the boundary, not a convenience.
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

METADATA_DIRNAME = "metadata"
CLINICAL_DIRNAME = "clinical"


class ValidationError(RuntimeError):
    """An operator-facing failure: bad arguments, unsafe paths, missing input."""


def quote_identifier(name: str) -> str:
    """
    Validate and quote a PostgreSQL identifier.

    Rejects rather than escapes. Every identifier these scripts handle is a
    schema or materialized-view name that should already be lower-snake-case, so
    anything else is a mistake worth surfacing at the boundary.
    """
    if not _IDENTIFIER.match(name):
        raise ValidationError(
            f"{name!r} is not a plain lower-case PostgreSQL identifier. "
            "Schema and view names must match [a-z_][a-z0-9_]*."
        )
    return f'"{name}"'


def qualified(schema: str, name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


# ---------------------------------------------------------------------------
# Engine and CDM resolution
# ---------------------------------------------------------------------------

def resolve_cdm_url(explicit: str | None = None) -> str:
    """
    Resolve the CDM connection URL.

    Order matches the test fixture and the package's own config path: an
    explicit argument, then the configured ``cdm_db`` resource, then
    ``ENGINE_CDM`` / ``ENGINE``. Sharing the order means an operator who can run
    the test suite can run these scripts without new configuration.
    """
    if explicit:
        return explicit

    try:
        from oa_configurator import Resolver, load_stack_config

        resolved = Resolver(load_stack_config()).resolve_database("cdm_db")
        if resolved.connection.url and resolved.connection.url.startswith("postgresql"):
            return resolved.connection.url
    except Exception:  # noqa: BLE001 - fall through to the environment
        pass

    url = os.getenv("ENGINE_CDM") or os.getenv("ENGINE")
    if not url:
        raise ValidationError(
            "No CDM connection available. Pass --cdm-url, configure a PostgreSQL "
            "cdm_db resource, or set ENGINE_CDM / ENGINE."
        )
    return url


def resolve_cdm_schema(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    try:
        from oa_configurator import Resolver, load_stack_config

        return (
            Resolver(load_stack_config()).resolve_database("cdm_db").schema_name
            or "public"
        )
    except Exception:  # noqa: BLE001
        return "public"


def create_engine(url: str) -> sa.Engine:
    engine = sa.create_engine(url, future=True)
    if not engine.url.drivername.startswith("postgresql"):
        engine.dispose()
        raise ValidationError(
            "These scripts are PostgreSQL-only: materialized views, "
            "pg_get_viewdef, and EXCEPT ALL comparison all depend on it. "
            f"Got {engine.url.drivername!r}."
        )
    return engine


def ensure_schema(engine: sa.Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema)}"))


def drop_schema(engine: sa.Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE"))


def schema_exists(engine: sa.Engine, schema: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                sa.text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.schemata "
                    "WHERE schema_name = :schema)"
                ),
                {"schema": schema},
            ).scalar()
        )


def matview_names(engine: sa.Engine, schema: str) -> tuple[str, ...]:
    with engine.connect() as conn:
        return tuple(
            conn.execute(
                sa.text(
                    "SELECT matviewname FROM pg_matviews WHERE schemaname = :schema "
                    "ORDER BY matviewname"
                ),
                {"schema": schema},
            )
            .scalars()
            .all()
        )


def matview_is_populated(engine: sa.Engine, schema: str, name: str) -> bool:
    """
    Whether a materialized view has data.

    A view created ``WITH NO DATA`` exists and reports its columns, but any
    SELECT against it raises. Building structurally first is the recommended
    safe opening move against an unfamiliar database, so this state is expected
    rather than exceptional and callers should report it, not crash on it.
    """
    with engine.connect() as conn:
        result = conn.execute(
            sa.text(
                """
                SELECT c.relispopulated
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :name AND c.relkind = 'm'
                """
            ),
            {"schema": schema, "name": name},
        ).scalar()
    return bool(result)


def matview_columns(engine: sa.Engine, schema: str, name: str) -> tuple[str, ...]:
    """
    Column names of a materialized view, in ordinal order.

    Read from ``pg_attribute`` rather than ``information_schema.columns``:
    materialized views are a PostgreSQL extension and do not appear in the
    information schema at all, so the standard view silently returns nothing for
    them. (``ConstructRegistry.validate()`` has the same defect — see OC-0-N9.)
    """
    with engine.connect() as conn:
        return tuple(
            conn.execute(
                sa.text(
                    """
                    SELECT a.attname
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = :schema
                      AND c.relname = :name
                      AND c.relkind IN ('m', 'v', 'r')
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum
                    """
                ),
                {"schema": schema, "name": name},
            )
            .scalars()
            .all()
        )


# ---------------------------------------------------------------------------
# Output routing
# ---------------------------------------------------------------------------

def _repo_root() -> Path | None:
    """Locate the repository root, so clinical writes into it can be refused."""
    here = Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "construct-contracts.toml").is_file() or (candidate / ".git").exists():
            return candidate
    return None


@dataclass(frozen=True)
class OutputPaths:
    """
    Where a run writes, split by disclosure class.

    ``metadata`` holds counts, checksums, and construct names. ``clinical``
    holds anything row-level or person-identifying. Callers must not write
    person data into ``metadata``.
    """

    root: Path
    metadata: Path
    clinical: Path


def prepare_output_dir(output_dir: str | Path, *, clinical: bool) -> OutputPaths:
    """
    Create and return the output layout, refusing unsafe locations.

    ``clinical=False`` runs are allowed anywhere: counts and checksums are what
    an operator hands back for review. ``clinical=True`` runs are refused inside
    the repository, because that is how patient data gets committed.
    """
    root = Path(output_dir).expanduser().resolve()

    if clinical:
        repo = _repo_root()
        if repo is not None and (root == repo or repo in root.parents):
            raise ValidationError(
                f"Refusing to write clinical output to {root}, which is inside the "
                f"repository at {repo}. Choose a directory in the secure "
                "environment's own storage."
            )

    paths = OutputPaths(
        root=root,
        metadata=root / METADATA_DIRNAME,
        clinical=root / CLINICAL_DIRNAME,
    )
    paths.metadata.mkdir(parents=True, exist_ok=True)
    if clinical:
        paths.clinical.mkdir(parents=True, exist_ok=True)
    return paths


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], *, headers: Sequence[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def utc_stamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_manifest(
    *,
    command: str,
    arguments: Mapping[str, Any],
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Describe a run so its outputs can be traced back to what produced them.

    Deliberately records the resolved specs and schema names but never the CDM
    URL, which carries credentials.
    """
    payload: dict[str, Any] = {
        "command": command,
        "generated_at": utc_stamp(),
        "arguments": dict(arguments),
    }
    if extra:
        payload.update(extra)
    return payload


def as_rows(items: Iterable[Any]) -> list[dict[str, Any]]:
    """Render a sequence of dataclass instances as CSV-ready dicts."""
    return [asdict(item) for item in items]
