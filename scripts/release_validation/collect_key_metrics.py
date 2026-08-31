#!/usr/bin/env python
"""Record the uniqueness, size, and definition baseline for one built schema.

For every construct in ``construct-contracts.toml`` that exists in the target
schema, this records:

- the deployed materialized-view definition checksum, from ``pg_get_viewdef``;
- the row count;
- how many rows have a NULL anywhere in the declared logical key;
- the number of distinct logical keys; and
- the resulting duplicate count.

This is the OC-0 baseline evidence. Declared keys in the manifest are *intended*
keys, so most constructs are expected to report duplicates here — that is the
measurement, not a failure. The exit status reflects whether the run completed,
not whether the keys hold. ``--fail-on-duplicates`` inverts that for use as a
release gate once OC-2 has landed.

Uniqueness is measured as ``count(*) = count(distinct (k1, ..., kn))``. That is
deliberately stricter than a PostgreSQL unique index: ``count(distinct)`` on a
row value treats NULLs as equal, so a construct with a null spine cannot hide
duplicate spine rows behind PostgreSQL's default NULL-distinct behaviour.

Output is non-clinical: counts, checksums, and construct names only.

    python scripts/release_validation/collect_key_metrics.py \
        --schema oc_candidate --output-dir /secure/oc0-run
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, TypedDict

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _sql_normalise import sql_checksum  # noqa: E402
from _common import (  # noqa: E402
    ValidationError,
    as_rows,
    create_engine,
    matview_columns,
    matview_is_populated,
    matview_names,
    prepare_output_dir,
    qualified,
    quote_identifier,
    resolve_cdm_url,
    run_manifest,
    write_csv,
    write_json,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from omop_constructs.core.contracts import load_contracts  # noqa: E402

METRIC_HEADERS = (
    "construct_name",
    "schema_name",
    "present",
    "definition_md5",
    "row_count",
    "null_key_rows",
    "distinct_keys",
    "duplicate_rows",
    "declared_key",
    "key_holds",
    "logical_key_complete",
    "known_violations",
    "public_api_1_0",
    "lung_report_role",
    "note",
)


@dataclass
class ConstructMetrics:
    """Measured state of one construct in one schema."""

    construct_name: str
    schema_name: str
    present: bool
    definition_md5: str | None = None
    row_count: int | None = None
    null_key_rows: int | None = None
    distinct_keys: int | None = None
    duplicate_rows: int | None = None
    declared_key: str = ""
    key_holds: bool | None = None
    logical_key_complete: bool = True
    known_violations: str = ""
    public_api_1_0: bool = True
    lung_report_role: str = ""
    note: str = ""


def definition_checksum(conn: sa.Connection, schema: str, name: str) -> str:
    """
    Checksum the deployed definition rather than the ORM-compiled select.

    ``pg_get_viewdef`` returns PostgreSQL's own normalised rendering, so the
    same logical query written slightly differently by two library versions
    still produces the same checksum, and a genuine change in the query always
    changes it.

    The definition text is normalised before hashing for two reasons. PostgreSQL
    faithfully preserves the order of an embedded concept-ID ``IN`` list, and that
    order comes from Python set iteration at build time, so the checksum would
    otherwise differ between two builds of the same version. PostgreSQL also
    stores the *qualified* name of every referenced object, so a construct built
    into a side schema names that schema in its own definition; stripping it is
    what makes the checksum comparable between a baseline and a candidate schema.
    """
    # cast(... AS regclass) rather than ::regclass: SQLAlchemy's text() bind
    # parser reads ":target::regclass" as a parameter named "target:" and passes
    # the colon through to PostgreSQL as a syntax error.
    definition = conn.execute(
        sa.text("SELECT pg_get_viewdef(cast(:target AS regclass), true)"),
        {"target": f"{schema}.{name}"},
    ).scalar_one()
    return sql_checksum(definition, strip_schema=schema)


def measure_construct(
    conn: sa.Connection,
    *,
    schema: str,
    name: str,
    logical_key: Sequence[str],
) -> tuple[str, int, int, int]:
    """Return (definition_md5, row_count, null_key_rows, distinct_keys)."""
    key_columns = [quote_identifier(column) for column in logical_key]
    null_test = " OR ".join(f"{column} IS NULL" for column in key_columns)
    key_tuple = ", ".join(key_columns)

    row = conn.execute(
        sa.text(
            f"""
            SELECT
                count(*)                                        AS row_count,
                count(*) FILTER (WHERE {null_test})             AS null_key_rows,
                count(DISTINCT ({key_tuple}))                   AS distinct_keys
            FROM {qualified(schema, name)}
            """
        )
    ).mappings().one()

    return (
        definition_checksum(conn, schema, name),
        row["row_count"],
        row["null_key_rows"],
        row["distinct_keys"],
    )


def collect(
    engine: sa.Engine,
    *,
    schema: str,
    contracts_path: Path | None,
    only_public: bool,
) -> list[ConstructMetrics]:
    contracts = load_contracts(contracts_path)
    present = set(matview_names(engine, schema))
    results: list[ConstructMetrics] = []

    with engine.connect() as conn:
        for contract in contracts:
            if only_public and not contract.public_api_1_0:
                continue

            metrics = ConstructMetrics(
                construct_name=contract.name,
                schema_name=schema,
                present=contract.name in present,
                declared_key="|".join(contract.logical_key),
                logical_key_complete=contract.logical_key_complete,
                known_violations="|".join(contract.known_violations),
                public_api_1_0=contract.public_api_1_0,
                lung_report_role=contract.lung_report_role,
            )

            if not metrics.present:
                metrics.note = "not present in schema"
                results.append(metrics)
                continue

            # A view built WITH NO DATA exists and has columns, but selecting
            # from it raises. Report it and move on: a structural-only build is
            # a legitimate first run, and it still yields useful checksums.
            if not matview_is_populated(engine, schema, contract.name):
                metrics.definition_md5 = definition_checksum(conn, schema, contract.name)
                metrics.note = "materialized view exists but is not populated"
                results.append(metrics)
                continue

            # A declared key naming a column the deployed view does not have is
            # recorded, not raised: the whole point of running this against two
            # versions is that one of them may predate a column.
            columns = set(matview_columns(engine, schema, contract.name))
            missing = [c for c in contract.logical_key if c not in columns]
            if missing:
                metrics.definition_md5 = definition_checksum(conn, schema, contract.name)
                metrics.note = f"declared key columns absent from view: {missing}"
                results.append(metrics)
                continue

            (
                metrics.definition_md5,
                metrics.row_count,
                metrics.null_key_rows,
                metrics.distinct_keys,
            ) = measure_construct(
                conn,
                schema=schema,
                name=contract.name,
                logical_key=contract.logical_key,
            )
            metrics.duplicate_rows = metrics.row_count - metrics.distinct_keys
            metrics.key_holds = metrics.duplicate_rows == 0
            results.append(metrics)

    return results


class MetricsSummary(TypedDict):
    constructs_in_manifest: int
    constructs_present: int
    constructs_measured: int
    keys_holding: int
    keys_violated: int
    violating_constructs: list[str]
    total_rows: int
    total_duplicate_rows: int
    total_null_key_rows: int
    unmeasured: list[dict[str, str]]


def summarise(results: Sequence[ConstructMetrics]) -> MetricsSummary:
    measured = [r for r in results if r.key_holds is not None]
    violating = [r for r in measured if not r.key_holds]
    return {
        "constructs_in_manifest": len(results),
        "constructs_present": sum(1 for r in results if r.present),
        "constructs_measured": len(measured),
        "keys_holding": len(measured) - len(violating),
        "keys_violated": len(violating),
        "violating_constructs": [r.construct_name for r in violating],
        "total_rows": sum(r.row_count or 0 for r in measured),
        "total_duplicate_rows": sum(r.duplicate_rows or 0 for r in measured),
        "total_null_key_rows": sum(r.null_key_rows or 0 for r in measured),
        "unmeasured": [
            {"construct_name": r.construct_name, "note": r.note}
            for r in results
            if r.key_holds is None
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record row counts, null-key counts, distinct logical-key counts, "
            "duplicate counts, and definition checksums for one built schema."
        )
    )
    parser.add_argument("--schema", required=True, help="Schema holding the built constructs.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write metadata into. Output is non-clinical.",
    )
    parser.add_argument("--cdm-url", help="PostgreSQL URL. Defaults to the configured cdm_db.")
    parser.add_argument(
        "--contracts",
        type=Path,
        help="Path to construct-contracts.toml. Defaults to the repository copy.",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Name for this run's output files. Defaults to the schema name.",
    )
    parser.add_argument(
        "--only-public",
        action="store_true",
        help="Restrict to constructs marked public_api_1_0 in the manifest.",
    )
    parser.add_argument(
        "--fail-on-duplicates",
        action="store_true",
        help=(
            "Exit non-zero when any measured construct violates its declared key. "
            "Off by default: at OC-0 the violations are the expected baseline."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    label = args.label or args.schema

    engine = create_engine(resolve_cdm_url(args.cdm_url))
    try:
        results = collect(
            engine,
            schema=args.schema,
            contracts_path=args.contracts,
            only_public=args.only_public,
        )
    finally:
        engine.dispose()

    paths = prepare_output_dir(args.output_dir, clinical=False)
    csv_path = write_csv(
        paths.metadata / f"key_metrics_{label}.csv",
        as_rows(results),
        headers=METRIC_HEADERS,
    )
    summary = summarise(results)
    json_path = write_json(
        paths.metadata / f"key_metrics_{label}.json",
        run_manifest(
            command="collect_key_metrics",
            arguments={
                "schema": args.schema,
                "label": label,
                "only_public": args.only_public,
            },
            extra={"summary": summary},
        ),
    )

    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")
    print(
        f"{summary['constructs_measured']} measured, "
        f"{summary['keys_holding']} keys hold, "
        f"{summary['keys_violated']} violated, "
        f"{summary['total_rows']:,} rows, "
        f"{summary['total_duplicate_rows']:,} duplicate rows"
    )
    if summary["violating_constructs"]:
        print("declared keys violated by:")
        for name in summary["violating_constructs"]:
            print(f"  - {name}")

    if args.fail_on_duplicates and summary["keys_violated"]:
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
