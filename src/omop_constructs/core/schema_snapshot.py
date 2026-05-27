from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable

import sqlalchemy as sa
from sqlalchemy.exc import CompileError

from ..bootstrap import get_complete_construct_registry
from .registry import ConstructRegistry


SNAPSHOT_HEADERS = (
    "construct_name",
    "module_name",
    "class_name",
    "kind",
    "deps",
    "mv_index",
    "mv_pk",
    "column_position",
    "column_name",
    "column_type",
    "nullable",
    "primary_key",
)


def _column_type_name(column: sa.Column) -> str:
    try:
        compiled = column.type.compile(dialect=sa.engine.default.DefaultDialect()) # type: ignore
    except:
        compiled = 'UNKNOWN'
    return str(compiled)


def registry_schema_rows(
    registry: ConstructRegistry,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    plan_by_name = {item.name: item for item in registry.plan()}

    for construct_name in sorted(plan_by_name):
        item = plan_by_name[construct_name]
        cls = registry.get(construct_name)
        deps = "|".join(item.deps)
        mv_index = getattr(cls, "__mv_index__", "") or ""
        mv_pk = "|".join(getattr(cls, "__mv_pk__", ()) or ())

        for position, column in enumerate(cls.__table__.columns, start=1): # type: ignore
            rows.append(
                {
                    "construct_name": construct_name,
                    "module_name": cls.__module__,
                    "class_name": cls.__name__,
                    "kind": item.kind,
                    "deps": deps,
                    "mv_index": mv_index,
                    "mv_pk": mv_pk,
                    "column_position": str(position),
                    "column_name": column.name,
                    "column_type": _column_type_name(column),
                    "nullable": str(column.nullable).lower(),
                    "primary_key": str(column.primary_key).lower(),
                }
            )

    return rows


def write_registry_schema_snapshot(
    output_path: str | Path,
    *,
    registry: ConstructRegistry | None = None,
) -> Path:
    registry = registry or get_complete_construct_registry()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SNAPSHOT_HEADERS)
        writer.writeheader()
        writer.writerows(registry_schema_rows(registry)) # type: ignore

    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export a CSV snapshot of construct mappers, columns, and SQLAlchemy "
            "column types for registry drift tracking."
        )
    )
    parser.add_argument(
        "output",
        help="Path to the CSV file to create or replace.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    path = write_registry_schema_snapshot(args.output)
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
