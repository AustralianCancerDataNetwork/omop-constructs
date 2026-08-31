#!/usr/bin/env python
"""Compare a baseline and a candidate side schema, construct by construct.

For every construct present in both schemas, this compares the columns they have
in common using ``EXCEPT ALL`` in both directions:

- rows in baseline but not candidate — removed or changed away;
- rows in candidate but not baseline — added or changed into.

``EXCEPT ALL`` rather than ``EXCEPT`` on purpose. Plain ``EXCEPT`` deduplicates,
which would hide a multiplicity change — the case where a construct returns the
same distinct rows but three copies instead of one. That is the OC-H1 and OC-B2
failure mode, so it is the thing most worth detecting.

Only columns present in both schemas are compared, and the ones dropped are
recorded. A construct whose column set changed cannot be compared column-for-
column, and silently comparing a subset without saying so would misreport a
schema change as a data match.

Differences are then summarised per construct and, where the construct exposes
``person_id``, per person, so an operator can see whether a delta touches a few
patients or the whole cohort.

Two output classes:

- ``metadata/`` — per-construct counts of added, removed, and net rows. Safe to
  hand back for review.
- ``clinical/`` — the differing rows themselves and the affected person
  identifiers. Written only with ``--emit-rows``, and refused inside the
  repository.

Also emits the generated SQL, so an operator can run and audit the comparison by
hand in the secure environment without trusting this script.

    python scripts/release_validation/compare_schemas.py \\
        --baseline-schema oc_transition_baseline \\
        --candidate-schema oc_transition_candidate \\
        --output-dir /secure/oc0-run --emit-rows
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    ValidationError,
    as_rows,
    create_engine,
    matview_columns,
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

DIFF_HEADERS = (
    "construct_name",
    "comparable",
    "compared_columns",
    "baseline_only_columns",
    "candidate_only_columns",
    "baseline_rows",
    "candidate_rows",
    "rows_only_in_baseline",
    "rows_only_in_candidate",
    "net_row_change",
    "persons_affected",
    "identical",
    "note",
)

#: Columns excluded from every comparison. A refresh-local row_number() differs
#: between two builds by construction, so including it would mark every row of
#: every construct as changed and tell an operator nothing.
REFRESH_LOCAL_COLUMNS = ("mv_id",)


@dataclass
class ConstructDiff:
    construct_name: str
    comparable: bool
    compared_columns: str = ""
    baseline_only_columns: str = ""
    candidate_only_columns: str = ""
    baseline_rows: int | None = None
    candidate_rows: int | None = None
    rows_only_in_baseline: int | None = None
    rows_only_in_candidate: int | None = None
    net_row_change: int | None = None
    persons_affected: int | None = None
    identical: bool | None = None
    note: str = ""


@dataclass
class GeneratedSql:
    """The SQL a comparison ran, kept so an operator can audit or rerun it."""

    construct_name: str
    baseline_only: str = ""
    candidate_only: str = ""
    person_summary: str = ""
    statements: list[str] = field(default_factory=list)


def except_all_sql(
    *,
    left_schema: str,
    right_schema: str,
    name: str,
    columns: Sequence[str],
) -> str:
    """Rows in left but not right, comparing only the given columns."""
    projection = ", ".join(quote_identifier(column) for column in columns)
    return (
        f"SELECT {projection} FROM {qualified(left_schema, name)}\n"
        f"EXCEPT ALL\n"
        f"SELECT {projection} FROM {qualified(right_schema, name)}"
    )


def person_summary_sql(
    *,
    baseline_schema: str,
    candidate_schema: str,
    name: str,
    columns: Sequence[str],
) -> str:
    """
    Per-person added and removed row counts for one construct.

    A person appears when either direction has rows for them, so a patient whose
    rows only changed shape still shows up with a non-zero count on both sides.
    """
    removed = except_all_sql(
        left_schema=baseline_schema,
        right_schema=candidate_schema,
        name=name,
        columns=columns,
    )
    added = except_all_sql(
        left_schema=candidate_schema,
        right_schema=baseline_schema,
        name=name,
        columns=columns,
    )
    return (
        f"WITH removed AS (\n{removed}\n),\n"
        f"added AS (\n{added}\n),\n"
        "per_person AS (\n"
        "    SELECT person_id, count(*) AS removed_rows, 0::bigint AS added_rows\n"
        "    FROM removed GROUP BY person_id\n"
        "  UNION ALL\n"
        "    SELECT person_id, 0::bigint AS removed_rows, count(*) AS added_rows\n"
        "    FROM added GROUP BY person_id\n"
        ")\n"
        "SELECT person_id,\n"
        "       sum(removed_rows) AS removed_rows,\n"
        "       sum(added_rows)   AS added_rows,\n"
        "       sum(added_rows) - sum(removed_rows) AS net_rows\n"
        "FROM per_person\n"
        "GROUP BY person_id\n"
        "ORDER BY abs(sum(added_rows) - sum(removed_rows)) DESC, person_id"
    )


def count_rows_sql(query: str) -> str:
    """Count a query's rows without transferring its payload to Python."""
    return f"SELECT count(*) FROM (\n{query}\n) AS result_rows"


def limited_rows_sql(query: str) -> str:
    """Return a database-bounded sample of a potentially large result."""
    return f"SELECT * FROM (\n{query}\n) AS result_rows LIMIT :row_limit"


def compare_construct(
    conn: sa.Connection,
    *,
    name: str,
    baseline_schema: str,
    candidate_schema: str,
    baseline_columns: Sequence[str],
    candidate_columns: Sequence[str],
    include_rows: bool = False,
    max_rows: int = 100_000,
) -> tuple[
    ConstructDiff,
    GeneratedSql,
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    """Compare one construct, optionally returning bounded clinical samples."""
    if max_rows < 1:
        raise ValidationError("max_rows must be at least 1")

    diff = ConstructDiff(construct_name=name, comparable=True)
    sql = GeneratedSql(construct_name=name)

    base_set, cand_set = set(baseline_columns), set(candidate_columns)
    shared = [
        column
        for column in baseline_columns
        if column in cand_set and column not in REFRESH_LOCAL_COLUMNS
    ]

    diff.baseline_only_columns = "|".join(sorted(base_set - cand_set))
    diff.candidate_only_columns = "|".join(sorted(cand_set - base_set))
    diff.compared_columns = "|".join(shared)

    diff.baseline_rows = int(
        conn.execute(
            sa.text(f"SELECT count(*) FROM {qualified(baseline_schema, name)}")
        ).scalar_one()
    )
    diff.candidate_rows = int(
        conn.execute(
            sa.text(f"SELECT count(*) FROM {qualified(candidate_schema, name)}")
        ).scalar_one()
    )
    diff.net_row_change = diff.candidate_rows - diff.baseline_rows

    if base_set != cand_set:
        diff.comparable = False
        diff.note = (
            "column sets differ; row equality was not evaluated against a shared subset"
        )
        return diff, sql, [], [], []

    if not shared:
        diff.comparable = False
        diff.note = "no comparable columns after excluding refresh-local identifiers"
        return diff, sql, [], [], []

    sql.baseline_only = except_all_sql(
        left_schema=baseline_schema,
        right_schema=candidate_schema,
        name=name,
        columns=shared,
    )
    sql.candidate_only = except_all_sql(
        left_schema=candidate_schema,
        right_schema=baseline_schema,
        name=name,
        columns=shared,
    )
    sql.statements = [sql.baseline_only, sql.candidate_only]

    diff.rows_only_in_baseline = int(
        conn.execute(sa.text(count_rows_sql(sql.baseline_only))).scalar_one()
    )
    diff.rows_only_in_candidate = int(
        conn.execute(sa.text(count_rows_sql(sql.candidate_only))).scalar_one()
    )
    diff.identical = (
        diff.rows_only_in_baseline == 0 and diff.rows_only_in_candidate == 0
    )

    removed_rows: list[dict[str, object]] = []
    added_rows: list[dict[str, object]] = []
    if include_rows:
        removed_rows = [
            dict(row)
            for row in conn.execute(
                sa.text(limited_rows_sql(sql.baseline_only)), {"row_limit": max_rows}
            ).mappings()
        ]
        added_rows = [
            dict(row)
            for row in conn.execute(
                sa.text(limited_rows_sql(sql.candidate_only)), {"row_limit": max_rows}
            ).mappings()
        ]

    person_rows: list[dict[str, object]] = []
    if "person_id" in shared:
        sql.person_summary = person_summary_sql(
            baseline_schema=baseline_schema,
            candidate_schema=candidate_schema,
            name=name,
            columns=shared,
        )
        sql.statements.append(sql.person_summary)
        diff.persons_affected = int(
            conn.execute(sa.text(count_rows_sql(sql.person_summary))).scalar_one()
        )
        if include_rows:
            person_rows = [
                dict(row)
                for row in conn.execute(
                    sa.text(limited_rows_sql(sql.person_summary)),
                    {"row_limit": max_rows},
                ).mappings()
            ]
    else:
        diff.note = "no person_id column; differences summarised by construct only"

    return diff, sql, removed_rows, added_rows, person_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two built construct schemas with EXCEPT ALL in both "
            "directions and summarise the differences by construct and person."
        )
    )
    parser.add_argument("--baseline-schema", required=True)
    parser.add_argument("--candidate-schema", required=True)
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write into. Per-construct counts go to metadata/; "
        "row-level output goes to clinical/ and requires --emit-rows.",
    )
    parser.add_argument("--cdm-url", help="PostgreSQL URL. Defaults to the configured cdm_db.")
    parser.add_argument("--contracts", type=Path, help="Path to construct-contracts.toml.")
    parser.add_argument(
        "--only-public",
        action="store_true",
        help="Restrict to constructs marked public_api_1_0 in the manifest.",
    )
    parser.add_argument(
        "--lung-only",
        action="store_true",
        help="Restrict to constructs the lung report reaches.",
    )
    parser.add_argument(
        "--emit-rows",
        action="store_true",
        help="Write the differing rows and affected person identifiers. This is "
        "clinical output and is refused inside the repository.",
    )
    parser.add_argument(
        "--max-rows-per-construct",
        type=int,
        default=100_000,
        help="Cap on differing rows written per construct per direction "
        "(default: 100000). Truncation is always reported, never silent.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if args.max_rows_per_construct < 1:
        raise ValidationError("--max-rows-per-construct must be at least 1")

    contracts = load_contracts(args.contracts)
    selected = [
        contract
        for contract in contracts
        if (not args.only_public or contract.public_api_1_0)
        and (not args.lung_only or contract.lung_report_role != "unused")
    ]

    paths = prepare_output_dir(args.output_dir, clinical=args.emit_rows)
    engine = create_engine(resolve_cdm_url(args.cdm_url))

    diffs: list[ConstructDiff] = []
    generated: list[GeneratedSql] = []
    truncated: list[dict[str, object]] = []

    try:
        baseline_present = set(matview_names(engine, args.baseline_schema))
        candidate_present = set(matview_names(engine, args.candidate_schema))
        if not baseline_present or not candidate_present:
            raise ValidationError(
                f"Expected materialized views in both schemas; found "
                f"{len(baseline_present)} in {args.baseline_schema} and "
                f"{len(candidate_present)} in {args.candidate_schema}. "
                "Run build_side_schema.py first."
            )

        with engine.connect() as conn:
            for contract in selected:
                name = contract.name
                if name not in baseline_present or name not in candidate_present:
                    diffs.append(
                        ConstructDiff(
                            construct_name=name,
                            comparable=False,
                            note=(
                                "present only in "
                                + (
                                    "candidate"
                                    if name in candidate_present
                                    else "baseline"
                                    if name in baseline_present
                                    else "neither schema"
                                )
                            ),
                        )
                    )
                    continue

                diff, sql, removed, added, persons = compare_construct(
                    conn,
                    name=name,
                    baseline_schema=args.baseline_schema,
                    candidate_schema=args.candidate_schema,
                    baseline_columns=matview_columns(engine, args.baseline_schema, name),
                    candidate_columns=matview_columns(engine, args.candidate_schema, name),
                    include_rows=args.emit_rows,
                    max_rows=args.max_rows_per_construct,
                )
                diffs.append(diff)
                generated.append(sql)

                if not args.emit_rows:
                    continue

                for label, rows in (("removed", removed), ("added", added)):
                    if not rows:
                        continue
                    total = (
                        diff.rows_only_in_baseline
                        if label == "removed"
                        else diff.rows_only_in_candidate
                    )
                    if total is not None and total > len(rows):
                        truncated.append(
                            {
                                "construct_name": name,
                                "direction": label,
                                "total_rows": total,
                                "written_rows": len(rows),
                            }
                        )
                    write_csv(
                        paths.clinical / f"{name}__{label}.csv",
                        rows,
                        headers=list(rows[0].keys()),
                    )
                if persons:
                    if (
                        diff.persons_affected is not None
                        and diff.persons_affected > len(persons)
                    ):
                        truncated.append(
                            {
                                "construct_name": name,
                                "direction": "persons",
                                "total_rows": diff.persons_affected,
                                "written_rows": len(persons),
                            }
                        )
                    write_csv(
                        paths.clinical / f"{name}__persons.csv",
                        persons,
                        headers=list(persons[0].keys()),
                    )
    finally:
        engine.dispose()

    write_csv(paths.metadata / "schema_comparison.csv", as_rows(diffs), headers=DIFF_HEADERS)

    sql_path = paths.metadata / "schema_comparison.sql"
    sql_path.write_text(
        "\n\n".join(
            f"-- {item.construct_name}\n" + ";\n\n".join(item.statements) + ";"
            for item in generated
            if item.statements
        )
        + "\n",
        encoding="utf-8",
    )

    compared = [d for d in diffs if d.comparable]
    differing = [d for d in compared if d.identical is False]
    summary = {
        "constructs_selected": len(diffs),
        "constructs_compared": len(compared),
        "constructs_identical": sum(1 for d in compared if d.identical),
        "constructs_differing": len(differing),
        "differing_constructs": [d.construct_name for d in differing],
        "not_comparable": [
            {"construct_name": d.construct_name, "note": d.note}
            for d in diffs
            if not d.comparable
        ],
        "total_rows_only_in_baseline": sum(d.rows_only_in_baseline or 0 for d in compared),
        "total_rows_only_in_candidate": sum(d.rows_only_in_candidate or 0 for d in compared),
        "truncated_outputs": truncated,
        "clinical_output_written": bool(args.emit_rows),
    }
    write_json(
        paths.metadata / "schema_comparison.json",
        run_manifest(
            command="compare_schemas",
            arguments={
                "baseline_schema": args.baseline_schema,
                "candidate_schema": args.candidate_schema,
                "only_public": args.only_public,
                "lung_only": args.lung_only,
                "emit_rows": args.emit_rows,
                "excluded_columns": list(REFRESH_LOCAL_COLUMNS),
            },
            extra={"summary": summary},
        ),
    )

    print(f"wrote {paths.metadata / 'schema_comparison.csv'}")
    print(f"wrote {sql_path}")
    print(
        f"{summary['constructs_compared']} compared, "
        f"{summary['constructs_identical']} identical, "
        f"{summary['constructs_differing']} differing"
    )
    for name in summary["differing_constructs"]:
        print(f"  differs: {name}")
    for item in truncated:
        print(
            f"  TRUNCATED {item['construct_name']} {item['direction']}: "
            f"wrote {item['written_rows']:,} of {item['total_rows']:,} rows"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
