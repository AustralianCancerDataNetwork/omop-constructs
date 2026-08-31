"""Render the contract manifest into the published construct catalogue.

`docs/construct-catalog.md` describes what each construct is *for*; that prose is
hand-written and stays hand-written. What this module generates is the part that
must not drift from `construct-contracts.toml`: the grain and logical key of
every construct, which ones are in the 1.0 public surface, which ones the lung
report reaches, and the findings register.

Generated content is written between marker comments so the surrounding prose
survives regeneration. A stale block is a test failure, not a silent
inconsistency — see ``tests/test_construct_catalogue_render.py``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

from .contracts import ConstructContract, ContractManifest, get_contracts
from .errors import ConstructSpecError

BEGIN_MARKER = "<!-- BEGIN GENERATED: construct-contracts -->"
END_MARKER = "<!-- END GENERATED: construct-contracts -->"

FAMILY_ORDER = ("episodes", "events", "modifiers", "demography")

_SURROGATE_LABEL = {
    "source_identifier": "source id",
    "refresh_local_row_number": "refresh-local",
    "none": "none",
}


def _escape(text: str) -> str:
    """Make a value safe inside a Markdown table cell."""
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _key_cell(contract: ConstructContract) -> str:
    columns = ", ".join(f"`{c}`" for c in contract.logical_key)
    if contract.key_nullable_columns:
        nullable = ", ".join(f"`{c}`" for c in contract.key_nullable_columns)
        columns += f"<br>nullable: {nullable}"
    if not contract.logical_key_complete:
        columns += "<br>**incomplete**"
    return columns


def _sorted_families(contracts: Iterable[ConstructContract]) -> list[tuple[str, list[ConstructContract]]]:
    grouped: dict[str, list[ConstructContract]] = {}
    for contract in contracts:
        grouped.setdefault(contract.family, []).append(contract)

    ordered = [f for f in FAMILY_ORDER if f in grouped]
    ordered += sorted(f for f in grouped if f not in FAMILY_ORDER)
    return [(family, sorted(grouped[family], key=lambda c: c.name)) for family in ordered]


def render_grain_tables(manifest: ContractManifest) -> list[str]:
    lines = [
        "### Declared grains and logical keys",
        "",
        "Every registered construct, by family. **Key** is the *intended* logical",
        "key — the one the construct must satisfy to enter a 1.0 release candidate.",
        "Where a construct does not satisfy it today, **Findings** names why and the",
        "register below explains each entry.",
        "",
        "**Surrogate** distinguishes an identifier carried through from the CDM,",
        "which is stable across refreshes and safe to reference, from a refresh-local",
        "`row_number()`, which is unique within one materialization only and must not",
        "be stored downstream.",
        "",
    ]

    for family, contracts in _sorted_families(manifest):
        lines += [
            f"#### {family.capitalize()}",
            "",
            "| Construct | Grain | Key | Surrogate | 1.0 | Lung | Findings |",
            "|---|---|---|---|---|---|---|",
        ]
        for contract in contracts:
            lines.append(
                "| `{name}` | {grain} | {key} | {surrogate} | {public} | {lung} | {findings} |".format(
                    name=contract.name,
                    grain=_escape(contract.grain),
                    key=_key_cell(contract),
                    surrogate=_SURROGATE_LABEL[contract.surrogate_kind],
                    public="yes" if contract.public_api_1_0 else "—",
                    lung={"direct": "direct", "dependency": "dep", "unused": "—"}[
                        contract.lung_report_role
                    ],
                    findings=", ".join(contract.known_violations) or "—",
                )
            )
        lines.append("")

    return lines


def render_public_surface(manifest: ContractManifest) -> list[str]:
    excluded = manifest.excluded_from_1_0()
    lines = [
        "### 1.0 public surface",
        "",
        f"{len(manifest.public_1_0())} of {len(manifest)} registered constructs are part of the",
        "public 1.0 surface: everything oa-cohorts imports, plus everything those",
        "constructs depend on. The rest stay registered and buildable but are not",
        "covered by the 1.0 contract, so their grain, keys, and column names may",
        "change without a compatibility cycle.",
        "",
        "**Not part of the 1.0 public surface:**",
        "",
    ]
    reasons: dict[str, list[str]] = {}
    for contract in excluded:
        reasons.setdefault(contract.public_api_note or "No reason recorded.", []).append(
            contract.name
        )

    for reason, names in reasons.items():
        lines.append(f"- {', '.join(f'`{n}`' for n in sorted(names))} — {_escape(reason)}")
    lines.append("")

    lung = manifest.lung_report_constructs()
    direct = [c for c in lung if c.lung_report_role == "direct"]
    lines += [
        "### Pre-production lung report",
        "",
        f"The lung report (`REP-000001`) resolves to {len(direct)} constructs directly and",
        f"reaches {len(lung)} in total once dependencies are included. Those are the",
        "constructs whose results a release must explain a before/after delta for.",
        "",
        "**Resolved directly by a lung report measure:**",
        "",
    ]
    for contract in sorted(direct, key=lambda c: c.name):
        targets = ", ".join(f"`{t}`" for t in contract.oa_cohorts_rule_targets)
        lines.append(f"- `{contract.name}` — {targets or 'via a payload query'}")
    lines.append("")
    return lines


def render_findings(manifest: ContractManifest) -> list[str]:
    lines = [
        "### Findings register",
        "",
        "`OC-B*`, `OC-H*`, and `OC-M*` are the joint review's own numbering.",
        "`OC-0-N*` were found while cataloguing grains for OC-0.",
        "",
        "| Finding | Severity | Scope | Summary | Constructs |",
        "|---|---|---|---|---|",
    ]
    for finding_id in sorted(manifest.findings):
        finding = manifest.findings[finding_id]
        affected = manifest.with_violation(finding_id)
        lines.append(
            "| `{fid}` | {severity} | {scope} | {summary} | {count} |".format(
                fid=finding_id,
                severity=finding.severity,
                scope=finding.scope,
                summary=_escape(finding.summary),
                count=len(affected) if affected else "—",
            )
        )
    lines.append("")
    return lines


def render_fan_out_notes(manifest: ContractManifest) -> list[str]:
    """Surface only the joins that multiply or drop rows.

    A reader tracing an unexpected row count needs the joins that change the row
    count, not a full join inventory.
    """
    lines = [
        "### Row-count behaviour of input joins",
        "",
        "Only joins that can change the row count are listed. A join that",
        "multiplies is where unexpected fan-out comes from; a join that drops rows",
        "is where unexpected *absence* comes from, which is harder to notice.",
        "",
    ]
    for contract in sorted(manifest, key=lambda c: (c.family, c.name)):
        interesting = contract.multiplying_inputs + contract.row_dropping_inputs
        if not interesting:
            continue
        lines += [f"**`{contract.name}`**", ""]
        for input_ in contract.multiplying_inputs:
            lines.append(f"- multiplies — {input_.name}: {_escape(input_.note or '')}")
        for input_ in contract.row_dropping_inputs:
            lines.append(f"- reduces — {input_.name}: {_escape(input_.note or '')}")
        if contract.fan_out_note:
            lines.append(f"- _{_escape(contract.fan_out_note)}_")
        lines.append("")
    return lines


def render(manifest: ContractManifest | None = None) -> str:
    """Render the complete generated block, markers included."""
    manifest = manifest or get_contracts()
    lines = [
        BEGIN_MARKER,
        "",
        "<!-- Generated from construct-contracts.toml. Do not edit by hand;",
        "     run `python -m omop_constructs.core.catalogue docs/construct-catalog.md`. -->",
        "",
        "## Construct contracts",
        "",
        "Generated from [`construct-contracts.toml`](https://github.com/AustralianCancerDataNetwork/omop-constructs/blob/main/construct-contracts.toml),",
        "the machine-readable grain catalogue. The manifest is the source of truth;",
        "this section is a rendering of it.",
        "",
    ]
    lines += render_grain_tables(manifest)
    lines += render_public_surface(manifest)
    lines += render_fan_out_notes(manifest)
    lines += render_findings(manifest)
    lines += [END_MARKER]
    return "\n".join(lines).rstrip() + "\n"


def splice(document: str, generated: str) -> str:
    """
    Replace the generated block in ``document``, or append it if absent.

    Markers rather than whole-file generation so the hand-written prose about
    what each construct is *for* survives regeneration.
    """
    if BEGIN_MARKER not in document:
        return document.rstrip() + "\n\n---\n\n" + generated

    if END_MARKER not in document:
        raise ConstructSpecError(
            f"Document has {BEGIN_MARKER!r} but no {END_MARKER!r}; refusing to guess "
            "where the generated block ends."
        )

    head, _, rest = document.partition(BEGIN_MARKER)
    _, _, tail = rest.partition(END_MARKER)
    return head + generated.rstrip() + tail


def write_catalogue(
    path: str | Path,
    *,
    manifest: ContractManifest | None = None,
) -> Path:
    target = Path(path)
    target.write_text(
        splice(target.read_text(encoding="utf-8"), render(manifest)),
        encoding="utf-8",
    )
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the construct contract manifest into the generated block of "
            "the construct catalogue."
        )
    )
    parser.add_argument("document", help="Markdown file to splice the generated block into.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the document is out of date instead of rewriting it.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    target = Path(args.document)
    current = target.read_text(encoding="utf-8")
    expected = splice(current, render())

    if args.check:
        if current != expected:
            print(
                f"{target} is out of date. Regenerate with:\n"
                f"  python -m omop_constructs.core.catalogue {target}"
            )
            return 1
        print(f"{target} is up to date")
        return 0

    target.write_text(expected, encoding="utf-8")
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
