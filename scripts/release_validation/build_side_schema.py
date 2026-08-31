#!/usr/bin/env python
"""Build a baseline and a candidate omop-constructs into two side schemas.

Takes two dependency specs, installs each into its own isolated environment,
and builds the full construct registry from each into its own PostgreSQL schema
over the same CDM. That is what makes an old/new comparison meaningful: only the
library version differs.

A spec is one or more requirements — local wheel paths, PyPI pins, or both —
given as repeatable ``--baseline`` / ``--candidate`` arguments. Each side gets
whatever it is given, so each carries its own omop-alchemy:

    python scripts/release_validation/build_side_schema.py \\
        --baseline 'omop-constructs==0.7.0' \\
        --candidate ../omop-constructs \\
        --candidate ../omop-alchemy/dist/omop_alchemy-1.1.1.dev5+ge7737e1c1-py3-none-any.whl \\
        --with-data \\
        --output-dir /secure/oc0-run

Both sides are built in dependency order using ``search_path`` to place the
views — see ``_build_driver.py`` for why, and why that is transitional.

Output is non-clinical: version numbers, per-construct compiled checksums, the
build plan, and any build failures. Populating the views does put clinical data
into the side schemas, which is why ``--with-data`` requires the operator to
name a schema prefix explicitly rather than accepting a default.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence, TypedDict

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    ValidationError,
    create_engine,
    drop_schema,
    matview_names,
    prepare_output_dir,
    quote_identifier,
    resolve_cdm_schema,
    resolve_cdm_url,
    run_manifest,
    write_json,
)

DRIVER = Path(__file__).resolve().parent / "_build_driver.py"


@dataclass
class SideResult:
    """Outcome of building one side."""

    side: str
    schema: str
    requirements: list[str]
    installed: dict[str, str] = field(default_factory=dict)
    plan: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    compiled_checksums: dict[str, str | None] = field(default_factory=dict)
    compiled_checksums_raw: dict[str, str | None] = field(default_factory=dict)
    columns: dict[str, list[str]] = field(default_factory=dict)
    error: str | None = None
    traceback: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and not self.failed


class DefinitionDiff(TypedDict):
    definition_changed: list[str]
    definition_unchanged: list[str]
    definition_ordering_only: list[str]
    constructs_added: list[str]
    constructs_removed: list[str]
    columns_changed: list[dict[str, object]]


def _require_uv() -> str:
    uv = shutil.which("uv")
    if uv is None:
        raise ValidationError(
            "uv is required to build the isolated side environments. Install it, "
            "or pre-build the environments and pass their interpreters with "
            "--baseline-python / --candidate-python."
        )
    return uv


def create_side_environment(
    venv_dir: Path,
    requirements: Sequence[str],
    *,
    python: str | None,
) -> Path:
    """
    Build an isolated environment holding exactly one version of the stack.

    Isolation is the point: installing both versions into one interpreter would
    make the comparison meaningless, because whichever imported first would win.
    """
    uv = _require_uv()
    subprocess.run(
        [uv, "venv", str(venv_dir)] + (["--python", python] if python else []),
        check=True,
        capture_output=True,
        text=True,
    )

    interpreter = venv_dir / "bin" / "python"
    if not interpreter.exists():  # Windows layout
        interpreter = venv_dir / "Scripts" / "python.exe"

    # psycopg comes from the postgres extra, but a bare wheel path bypasses
    # extras, so it is requested explicitly rather than left to chance.
    install = subprocess.run(
        [uv, "pip", "install", "--python", str(interpreter), *requirements, "psycopg[binary]"],
        capture_output=True,
        text=True,
    )
    if install.returncode != 0:
        raise ValidationError(
            f"Failed to install {list(requirements)} into {venv_dir}:\n"
            f"{install.stdout}\n{install.stderr}"
        )
    return interpreter


def installed_versions(interpreter: Path) -> dict[str, str]:
    program = (
        "import json\n"
        "from importlib.metadata import version, PackageNotFoundError\n"
        "out = {}\n"
        "for name in ('omop-constructs', 'omop-alchemy', 'omop-semantics', "
        "'oa-configurator', 'orm-loader', 'sqlalchemy'):\n"
        "    try:\n"
        "        out[name] = version(name)\n"
        "    except PackageNotFoundError:\n"
        "        out[name] = 'not installed'\n"
        "print(json.dumps(out))\n"
    )
    result = subprocess.run(
        [str(interpreter), "-c", program], capture_output=True, text=True
    )
    if result.returncode != 0:
        return {}
    return json.loads(result.stdout)


def build_driver_environment(interpreter: Path, cdm_url: str) -> dict[str, str]:
    """Pin every resolver used by a side build to the explicitly selected CDM.

    Construct modules resolve concepts while they are imported. Both the old
    and candidate package therefore need their stack config disabled before
    Python starts; passing ``--cdm-url`` only controls the later DDL connection.
    An existing but deliberately invalid config makes all supported package
    versions reach their established ENGINE_CDM fallback. A missing path is not
    sufficient: current oa-configurator rejects an explicit missing
    ``OA_CONFIG_PATH`` while its module is imported, before omop-constructs can
    catch the later config-load failure.
    """
    fallback_config = interpreter.parent / ".oc0-explicit-cdm-fallback.toml"
    sentinel = "# Forces ENGINE_CDM fallback for the isolated OC-0 build.\ninvalid = [\n"
    if fallback_config.exists() and fallback_config.read_text(encoding="utf-8") != sentinel:
        raise ValidationError(
            f"Release-validation sentinel has unexpected content: {fallback_config}"
        )
    fallback_config.write_text(sentinel, encoding="utf-8")
    fallback_config.chmod(0o600)

    environment = os.environ.copy()
    environment["OA_CONFIG_PATH"] = str(fallback_config)
    environment["ENGINE_CDM"] = cdm_url
    environment["ENGINE"] = cdm_url
    return environment


def build_side(
    *,
    side: str,
    requirements: Sequence[str],
    interpreter: Path,
    schema: str,
    cdm_url: str,
    cdm_schema: str,
    with_data: bool,
) -> SideResult:
    result = SideResult(side=side, schema=schema, requirements=list(requirements))
    result.installed = installed_versions(interpreter)

    command = [
        str(interpreter),
        str(DRIVER),
        "--cdm-url",
        cdm_url,
        "--target-schema",
        schema,
        "--cdm-schema",
        cdm_schema,
    ]
    if with_data:
        command.append("--with-data")

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=build_driver_environment(interpreter, cdm_url),
    )

    payload: dict = {}
    if completed.stdout.strip():
        try:
            payload = json.loads(completed.stdout.strip().splitlines()[-1])
        except json.JSONDecodeError:
            payload = {}

    if not payload:
        result.error = (
            f"build driver produced no parseable result (exit {completed.returncode})"
        )
        result.traceback = f"stdout:\n{completed.stdout}\n\nstderr:\n{completed.stderr}"
        return result

    result.plan = payload.get("plan", [])
    result.created = payload.get("created", [])
    result.failed = payload.get("failed", [])
    result.compiled_checksums = {
        c["name"]: c.get("compiled_md5") for c in payload.get("constructs", [])
    }
    result.compiled_checksums_raw = {
        c["name"]: c.get("compiled_md5_raw") for c in payload.get("constructs", [])
    }
    result.columns = {c["name"]: c.get("columns", []) for c in payload.get("constructs", [])}
    if "error" in payload:
        result.error = payload["error"]
        result.traceback = payload.get("traceback") or completed.stderr
    return result


def compare_definitions(baseline: SideResult, candidate: SideResult) -> DefinitionDiff:
    """
    Diff the two builds structurally, before any row is compared.

    A construct whose compiled checksum is unchanged cannot have changed its
    results, so this narrows what the row-level comparison has to look at — and
    a changed column set explains a comparison that cannot be run at all.

    Comparison uses the *normalised* checksum. Constructs that embed a resolved
    concept-ID set render that set in hash-seed-dependent order, so the raw
    checksum differs between any two processes and would report a large share of
    the registry as changed every run. Where only the raw checksum differs, the
    construct is reported under ``ordering_only`` rather than as a change.
    """
    names = sorted(set(baseline.compiled_checksums) | set(candidate.compiled_checksums))
    changed, unchanged, added, removed, columns_changed = [], [], [], [], []
    ordering_only = []

    for name in names:
        in_baseline = name in baseline.compiled_checksums
        in_candidate = name in candidate.compiled_checksums
        if in_baseline and not in_candidate:
            removed.append(name)
            continue
        if in_candidate and not in_baseline:
            added.append(name)
            continue

        if baseline.compiled_checksums[name] == candidate.compiled_checksums[name]:
            unchanged.append(name)
            if baseline.compiled_checksums_raw.get(name) != candidate.compiled_checksums_raw.get(
                name
            ):
                ordering_only.append(name)
        else:
            changed.append(name)

        base_cols, cand_cols = baseline.columns.get(name, []), candidate.columns.get(name, [])
        if base_cols != cand_cols:
            columns_changed.append(
                {
                    "construct_name": name,
                    "added_columns": sorted(set(cand_cols) - set(base_cols)),
                    "removed_columns": sorted(set(base_cols) - set(cand_cols)),
                }
            )

    return {
        "definition_changed": changed,
        "definition_unchanged": unchanged,
        "definition_ordering_only": ordering_only,
        "constructs_added": added,
        "constructs_removed": removed,
        "columns_changed": columns_changed,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Install a baseline and a candidate omop-constructs into isolated "
            "environments and build each into its own PostgreSQL schema."
        )
    )
    parser.add_argument(
        "--baseline",
        action="append",
        required=True,
        metavar="REQUIREMENT",
        help="Requirement for the baseline side. Repeatable: wheel paths, source "
        "directories, or PyPI pins. Include the baseline omop-alchemy here.",
    )
    parser.add_argument(
        "--candidate",
        action="append",
        required=True,
        metavar="REQUIREMENT",
        help="Requirement for the candidate side. Repeatable. Include the "
        "candidate omop-alchemy wheel here.",
    )
    parser.add_argument(
        "--schema-prefix",
        default="oc_transition",
        help="Prefix for the two side schemas (default: oc_transition). "
        "The schemas are <prefix>_baseline and <prefix>_candidate.",
    )
    parser.add_argument("--cdm-url", help="PostgreSQL URL. Defaults to the configured cdm_db.")
    parser.add_argument("--cdm-schema", help="CDM schema. Defaults to the configured one.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write build metadata into. Output is non-clinical.",
    )
    parser.add_argument(
        "--with-data",
        action="store_true",
        help="Populate the views. Without it the build is structural only, which "
        "is enough to compare definitions and column sets but not rows.",
    )
    parser.add_argument(
        "--venv-root",
        type=Path,
        help="Where to create the side environments. Defaults to <output-dir>/venvs.",
    )
    parser.add_argument("--baseline-python", help="Interpreter version for the baseline venv.")
    parser.add_argument("--candidate-python", help="Interpreter version for the candidate venv.")
    parser.add_argument(
        "--replace-schemas",
        action="store_true",
        help="Drop the two side schemas first. Required to re-run over an "
        "existing pair; without it an existing schema is an error.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Build the candidate even if the baseline build failed.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    baseline_schema = f"{args.schema_prefix}_baseline"
    candidate_schema = f"{args.schema_prefix}_candidate"
    for schema in (baseline_schema, candidate_schema):
        quote_identifier(schema)  # validate early, before anything is installed

    cdm_url = resolve_cdm_url(args.cdm_url)
    cdm_schema = resolve_cdm_schema(args.cdm_schema)
    quote_identifier(cdm_schema)
    if cdm_schema in {baseline_schema, candidate_schema}:
        raise ValidationError(
            f"Side schema collides with the CDM schema {cdm_schema!r}. "
            "Choose a different --schema-prefix."
        )

    paths = prepare_output_dir(args.output_dir, clinical=False)
    venv_root = args.venv_root or (paths.root / "venvs")
    venv_root.mkdir(parents=True, exist_ok=True)

    engine = create_engine(cdm_url)
    try:
        if args.replace_schemas:
            for schema in (baseline_schema, candidate_schema):
                drop_schema(engine, schema)
        else:
            existing = [
                schema
                for schema in (baseline_schema, candidate_schema)
                if matview_names(engine, schema)
            ]
            if existing:
                raise ValidationError(
                    f"Side schemas already hold materialized views: {existing}. "
                    "Pass --replace-schemas to rebuild them."
                )

        sides: dict[str, SideResult] = {}
        for side, requirements, schema, python in (
            ("baseline", args.baseline, baseline_schema, args.baseline_python),
            ("candidate", args.candidate, candidate_schema, args.candidate_python),
        ):
            print(f"[{side}] creating environment for {requirements}")
            interpreter = create_side_environment(
                venv_root / side, requirements, python=python
            )
            print(f"[{side}] building into {schema}")
            result = build_side(
                side=side,
                requirements=requirements,
                interpreter=interpreter,
                schema=schema,
                cdm_url=cdm_url,
                cdm_schema=cdm_schema,
                with_data=args.with_data,
            )
            sides[side] = result
            print(
                f"[{side}] {len(result.created)}/{len(result.plan)} views created"
                + (f" — FAILED: {result.error}" if result.error else "")
            )
            if not result.ok and not args.keep_going and side == "baseline":
                break
    finally:
        engine.dispose()

    definition_diff = (
        compare_definitions(sides["baseline"], sides["candidate"])
        if "candidate" in sides
        else {}
    )

    payload = run_manifest(
        command="build_side_schema",
        arguments={
            "baseline": args.baseline,
            "candidate": args.candidate,
            "baseline_schema": baseline_schema,
            "candidate_schema": candidate_schema,
            "cdm_schema": cdm_schema,
            "with_data": args.with_data,
        },
        extra={
            "sides": {
                name: {
                    "schema": r.schema,
                    "requirements": r.requirements,
                    "installed": r.installed,
                    "plan": r.plan,
                    "created": r.created,
                    "failed": r.failed,
                    "compiled_checksums": r.compiled_checksums,
                    "compiled_checksums_raw": r.compiled_checksums_raw,
                    "error": r.error,
                    "traceback": r.traceback,
                }
                for name, r in sides.items()
            },
            "definition_diff": definition_diff,
        },
    )
    json_path = write_json(paths.metadata / "side_schema_build.json", payload)
    print(f"wrote {json_path}")

    if definition_diff:
        print(
            f"definitions changed: {len(definition_diff['definition_changed'])}, "
            f"unchanged: {len(definition_diff['definition_unchanged'])}, "
            f"added: {len(definition_diff['constructs_added'])}, "
            f"removed: {len(definition_diff['constructs_removed'])}"
        )
        for name in definition_diff["definition_changed"]:
            print(f"  changed: {name}")
        if definition_diff["definition_ordering_only"]:
            print(
                f"  {len(definition_diff['definition_ordering_only'])} construct(s) differ "
                "only in embedded concept-list ordering, not in meaning"
            )

    return 0 if all(r.ok for r in sides.values()) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
