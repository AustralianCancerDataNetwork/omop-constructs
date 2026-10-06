"""Render the contract manifest into the published construct catalogue.

The catalogue is an index page, `docs/construct-catalog.md`, plus one page per
construct family under `docs/constructs/`. Each page has hand-written prose about
what its constructs are *for*; that prose stays hand-written. What this module
generates is the part that must not drift from `construct-contracts.toml`: what
one row of each construct represents, its key, and whether its row IDs are
stable.

The pages describe the package as it is now. Review findings, release planning
and report-specific coverage stay in the manifest and are not rendered.

Generated content is written between marker comments on each page so the
surrounding prose survives regeneration. A stale block is a test failure, not a
silent inconsistency — see ``tests/test_construct_catalogue_render.py``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

from .contracts import ConstructContract, ContractManifest, get_contracts
from .errors import ConstructSpecError

BEGIN_MARKER = "<!-- BEGIN GENERATED: construct-contracts -->"
END_MARKER = "<!-- END GENERATED: construct-contracts -->"

INDEX_PAGE = "construct-catalog.md"
FAMILY_ORDER = ("episodes", "events", "modifiers", "demography")
_FAMILY_TITLE = {
    "episodes": "Episode constructs",
    "events": "Event constructs",
    "modifiers": "Modifier constructs",
    "demography": "Demography constructs",
}

_REGENERATE = "python -m omop_constructs.core.catalogue docs/construct-catalog.md"

_ROW_ID_TEXT = {
    "source_identifier": "Taken from the CDM, so they stay the same across refreshes and are safe to store.",
    "refresh_local_row_number": "Numbered afresh on every refresh. Do not store them or join on them across refreshes.",
    "none": "This construct has no row ID column.",
}


def _escape(text: str) -> str:
    """Make manifest prose safe inside Markdown, including table cells."""
    return text.replace("|", "\\|").replace("*", "\\*").replace("\n", " ").strip()


def family_page(family: str) -> str:
    """The docs-relative path of a family's page."""
    return f"constructs/{family}.md"


def family_title(family: str) -> str:
    return _FAMILY_TITLE.get(family, f"{family.capitalize()} constructs")


def _anchor(contract: ConstructContract) -> str:
    return f"contract-{contract.name}"


def _link(contract: ConstructContract) -> str:
    return f"[`{contract.class_name}`](#{_anchor(contract)})"


def _grain(contract: ConstructContract) -> str:
    """Return what one row is today: ``current_grain`` when the manifest has one."""
    text = (contract.current_grain or contract.grain).strip().removeprefix("Declared:").strip()
    return text[:1].upper() + text[1:]


def _row_unit(contract: ConstructContract) -> str:
    """Shorten the grain sentence to what one row represents, for the overview."""
    text = _grain(contract)
    if text.lower().startswith("one row per "):
        text = text[len("one row per ") :]
    if text.startswith("(") and ")" in text:
        return " + ".join(part.strip() for part in text[1 : text.index(")")].split(","))
    for stop in (",", ":", "."):
        if stop in text:
            text = text[: text.index(stop)]
    return text.strip()


def _sorted_families(contracts: Iterable[ConstructContract]) -> list[tuple[str, list[ConstructContract]]]:
    grouped: dict[str, list[ConstructContract]] = {}
    for contract in contracts:
        grouped.setdefault(contract.family, []).append(contract)

    ordered = [f for f in FAMILY_ORDER if f in grouped]
    ordered += sorted(f for f in grouped if f not in FAMILY_ORDER)
    return [
        (family, sorted(grouped[family], key=lambda c: c.class_name.lower()))
        for family in ordered
    ]


def _generated_comment() -> str:
    return f"<!-- Generated from construct-contracts.toml. Do not edit by hand; run `{_REGENERATE}`. -->"


def render(manifest: ContractManifest | None = None) -> str:
    """Render the index page's generated block, markers included."""
    manifest = manifest or get_contracts()
    lines = [
        BEGIN_MARKER,
        "",
        _generated_comment(),
        "",
        "## Constructs by family",
        "",
        f"The package registers {len(manifest)} constructs. Each family page describes its constructs and lists what one row represents, how rows are identified, and whether row IDs can be stored.",
        "",
        "| Family | Constructs |",
        "|---|---|",
    ]
    for family, contracts in _sorted_families(manifest):
        lines.append(f"| [{family_title(family)}]({family_page(family)}) | {len(contracts)} |")
    lines.append("")

    lines += [END_MARKER]
    return "\n".join(lines).rstrip() + "\n"


def _unique_on(contract: ConstructContract) -> str:
    if contract.current_unique_on:
        return contract.current_unique_on.strip()
    text = ", ".join(f"`{c}`" for c in contract.logical_key)
    if contract.key_nullable_columns:
        nullable = ", ".join(f"`{c}`" for c in contract.key_nullable_columns)
        verb = "is" if len(contract.key_nullable_columns) == 1 else "are"
        text += f". {nullable} {verb} NULL on rows with nothing to link"
    return text + "."


def render_card(contract: ConstructContract) -> list[str]:
    return [
        f"### `{contract.class_name}` {{#{_anchor(contract)}}}",
        "",
        f"View: `{contract.name}`",
        "",
        f"**Rows:** {_grain(contract)}",
        "",
        f"**Unique on:** {_unique_on(contract)}",
        "",
        f"**Row IDs:** {_ROW_ID_TEXT[contract.surrogate_kind]}",
        "",
    ]


def render_family(manifest: ContractManifest, family: str) -> str:
    """Render one family page's generated block, markers included."""
    contracts = dict(_sorted_families(manifest)).get(family)
    if not contracts:
        raise ConstructSpecError(f"construct-contracts.toml has no constructs in family {family!r}")
    lines = [
        BEGIN_MARKER,
        "",
        _generated_comment(),
        "",
        "## Contracts",
        "",
        "What one row of each construct represents and how rows are identified.",
        "",
        "- **Rows**: what one row represents.",
        "- **Unique on**: the columns that together identify one row.",
        "- **Row IDs**: whether the construct's own ID column can be stored and reused.",
        "",
        "| Construct | One row per |",
        "|---|---|",
    ]
    for contract in contracts:
        lines.append(f"| {_link(contract)} | {_escape(_row_unit(contract))} |")
    lines.append("")
    for contract in contracts:
        lines += render_card(contract)
    lines += [END_MARKER]
    return "\n".join(lines).rstrip() + "\n"


def render_pages(manifest: ContractManifest | None = None) -> dict[str, str]:
    """Every generated block, keyed by its docs-relative page path."""
    manifest = manifest or get_contracts()
    pages = {INDEX_PAGE: render(manifest)}
    for family, _ in _sorted_families(manifest):
        pages[family_page(family)] = render_family(manifest, family)
    return pages


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


def sync_catalogue(
    index: str | Path,
    *,
    manifest: ContractManifest | None = None,
    write: bool = True,
) -> list[Path]:
    """Bring every catalogue page in step with the manifest.

    ``index`` is the catalogue's index page; family pages are resolved relative
    to its directory. Returns the pages that were out of date. With
    ``write=False`` nothing is changed, so the result is a staleness check.
    """
    docs = Path(index).parent
    stale: list[Path] = []
    for relative, block in render_pages(manifest).items():
        page = docs / relative
        current = page.read_text(encoding="utf-8") if page.exists() else ""
        expected = splice(current, block)
        if current == expected:
            continue
        stale.append(page)
        if write:
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text(expected, encoding="utf-8")
    return stale


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the construct contract manifest into the generated blocks of "
            "the construct catalogue and its family pages."
        )
    )
    parser.add_argument(
        "document",
        help="The catalogue index page, normally docs/construct-catalog.md. Family pages are written beside it.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any page is out of date instead of rewriting it.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    stale = sync_catalogue(args.document, write=not args.check)

    if args.check:
        if stale:
            pages = "\n".join(f"  {page}" for page in stale)
            print(f"Out of date:\n{pages}\nRegenerate with:\n  {_REGENERATE}")
            return 1
        print("Construct catalogue is up to date")
        return 0

    for page in stale:
        print(page)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
