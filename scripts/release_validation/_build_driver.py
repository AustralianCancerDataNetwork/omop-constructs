#!/usr/bin/env python
"""Build one omop-constructs version into one schema. Runs INSIDE a side venv.

This is invoked by ``build_side_schema.py`` with the interpreter of an isolated
environment that has a *specific* omop-constructs and omop-alchemy installed. It
therefore cannot import anything from the checkout it lives in, and must only
use API that exists in every version being compared. The whole surface it relies
on is ``get_complete_construct_registry()``, ``registry.plan()``, and
``create_mv()`` — all present since 0.7.0.

Placement in the target schema uses ``search_path``. The registry emits
unqualified DDL (``CREATE MATERIALIZED VIEW condition_episode_mv AS ...``), so
with ``search_path = <side schema>, <cdm schema>`` the views are created in the
side schema while their CDM inputs resolve from the CDM schema, and each view
that reads another one finds the copy in its own side schema. That is exactly
the schema-dependence OC-B5 identifies as a defect; here it is the mechanism
that makes a side-by-side build possible at all, which is why this script is
transitional and dies with OC-3.

Reports one JSON object on stdout so the orchestrator does not have to parse
logs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback

# Import by path rather than by package: this file is executed by an interpreter
# from an isolated side environment, so the directory it lives in is on sys.path
# but nothing else from the checkout is importable.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _common import quote_identifier  # noqa: E402
from _sql_normalise import raw_checksum, sql_checksum  # noqa: E402


def compiled_definition_checksums(cls) -> tuple[str | None, str | None]:
    """
    Checksum the ORM-compiled select, before any database sees it.

    Complements the deployed ``pg_get_viewdef`` checksum: this one shows whether
    the two library versions *intended* the same query, independently of how
    PostgreSQL chooses to render it.

    Returns (normalised, raw). The normalised checksum sorts embedded concept-ID
    IN lists, which are otherwise rendered in hash-seed-dependent set order and
    would make a quarter of the registry look changed on every run. The raw one
    is kept so a run can tell "the query changed" from "only the ordering did".
    """
    try:
        from sqlalchemy.dialects import postgresql

        sql = str(
            cls.__mv_select__.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )
    except Exception:  # noqa: BLE001 - a version that cannot compile is a result
        return None, None
    return sql_checksum(sql), raw_checksum(sql)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdm-url", required=True)
    parser.add_argument("--target-schema", required=True)
    parser.add_argument("--cdm-schema", required=True)
    parser.add_argument(
        "--with-data",
        action="store_true",
        help="Populate the views. Without it the build is structural only.",
    )
    args = parser.parse_args(argv)
    target_schema = quote_identifier(args.target_schema)
    cdm_schema = quote_identifier(args.cdm_schema)

    import sqlalchemy as sa

    from omop_constructs import get_complete_construct_registry

    result: dict[str, object] = {
        "target_schema": args.target_schema,
        "with_data": args.with_data,
        "constructs": [],
        "created": [],
        "failed": [],
    }

    try:
        import omop_alchemy
        from importlib.metadata import version

        result["versions"] = {
            "omop_constructs": version("omop-constructs"),
            "omop_alchemy": version("omop-alchemy"),
            "omop_alchemy_path": getattr(omop_alchemy, "__file__", None),
        }
    except Exception:  # noqa: BLE001
        result["versions"] = {}

    registry = get_complete_construct_registry()
    plan = list(registry.plan())
    result["plan"] = [item.name for item in plan]

    for item in plan:
        cls = registry.get(item.name)
        normalised, raw = compiled_definition_checksums(cls)
        result["constructs"].append(
            {
                "name": item.name,
                "deps": list(item.deps),
                "compiled_md5": normalised,
                "compiled_md5_raw": raw,
                "columns": list(cls.__table__.columns.keys()),
            }
        )

    engine = sa.create_engine(args.cdm_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

        # One connection and one transaction for the whole build, so a failure
        # part-way leaves no half-populated schema behind.
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"SET LOCAL search_path TO {target_schema}, {cdm_schema}"
                )
            )
            for item in plan:
                cls = registry.get(item.name)
                try:
                    cls.create_mv(conn, with_data=args.with_data)
                except Exception as exc:  # noqa: BLE001
                    result["failed"].append(
                        {
                            "name": item.name,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    # Abort rather than continue: everything after this point in
                    # the plan depends on views that may now be missing, and the
                    # transaction is aborted anyway.
                    raise
                result["created"].append(item.name)
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()
        print(json.dumps(result))
        return 1
    finally:
        engine.dispose()

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
