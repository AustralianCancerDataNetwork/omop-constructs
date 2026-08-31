from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .config import OmopConstructsConfig
from .core.catalogue import render, splice
from .core.contracts import get_contracts
from .core.schema_snapshot import write_registry_schema_snapshot


def _schema_snapshot_command(args: argparse.Namespace) -> int:
    path = write_registry_schema_snapshot(args.output)
    print(path)
    return 0


def _render_catalogue_command(args: argparse.Namespace) -> int:
    """Splice the generated contract block into a catalogue document."""
    document: Path = args.document
    current = document.read_text(encoding="utf-8")
    expected = splice(current, render())

    if args.check:
        if current != expected:
            print(
                f"{document} is out of date with the construct contract manifest. "
                f"Regenerate it with:\n  omop-constructs render-catalogue {document}"
            )
            return 1
        print(f"{document} is up to date")
        return 0

    document.write_text(expected, encoding="utf-8")
    print(document)
    return 0


def _contracts_command(args: argparse.Namespace) -> int:
    """Summarise the contract manifest without needing a database."""
    contracts = get_contracts()
    print(f"{contracts.path}")
    print(
        f"  {len(contracts)} constructs, {len(contracts.public_1_0())} in the 1.0 public "
        f"surface, {len(contracts.lung_report_constructs())} reachable from the lung report"
    )

    unsatisfied = [c for c in contracts if not c.satisfies_declared_key]
    finding_bearing = [c for c in contracts if c.known_violations]
    print(
        f"  {len(contracts) - len(unsatisfied)} constructs expected to satisfy "
        "their declared key"
    )
    print(f"  {len(unsatisfied)} not expected to satisfy their declared key")
    print(f"  {len(finding_bearing)} with recorded findings of any kind")

    if not args.verbose_contracts:
        return 0

    for contract in sorted(contracts, key=lambda c: (c.family, c.name)):
        flags = []
        if not contract.public_api_1_0:
            flags.append("not-1.0")
        if contract.lung_report_role != "unused":
            flags.append(f"lung:{contract.lung_report_role}")
        if not contract.logical_key_complete:
            flags.append("incomplete-key")
        print(
            f"  - {contract.name:34s} key=({', '.join(contract.logical_key)})"
            + (f" [{', '.join(flags)}]" if flags else "")
            + (f" findings={', '.join(contract.known_violations)}" if contract.known_violations else "")
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="omop-constructs",
        description="CLI utilities for omop-constructs.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase log verbosity (-v INFO, -vv DEBUG).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot_parser = subparsers.add_parser(
        "schema-snapshot",
        help="Export a CSV snapshot of the registered construct schema.",
    )
    snapshot_parser.add_argument(
        "output",
        type=Path,
        help="Path to the CSV file to create or replace.",
    )
    snapshot_parser.set_defaults(handler=_schema_snapshot_command)

    catalogue_parser = subparsers.add_parser(
        "render-catalogue",
        help=(
            "Splice the generated construct-contract block into a catalogue "
            "document. The manifest is the source of truth for grains and keys."
        ),
    )
    catalogue_parser.add_argument(
        "document",
        type=Path,
        help="Markdown file to update, normally docs/construct-catalog.md.",
    )
    catalogue_parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the document is stale instead of rewriting it.",
    )
    catalogue_parser.set_defaults(handler=_render_catalogue_command)

    contracts_parser = subparsers.add_parser(
        "contracts",
        help="Summarise the construct contract manifest. Needs no database.",
    )
    contracts_parser.add_argument(
        "--verbose-contracts",
        action="store_true",
        help="List every construct with its declared key and recorded findings.",
    )
    contracts_parser.set_defaults(handler=_contracts_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    OmopConstructsConfig.configure_logging(verbosity=args.verbose)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
