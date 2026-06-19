from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .config import OmopConstructsConfig
from .core.schema_snapshot import write_registry_schema_snapshot


def _schema_snapshot_command(args: argparse.Namespace) -> int:
    path = write_registry_schema_snapshot(args.output)
    print(path)
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
