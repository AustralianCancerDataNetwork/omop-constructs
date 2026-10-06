"""Keep the published catalogue in step with the contract manifest.

The catalogue is `docs/construct-catalog.md` plus one page per construct family
under `docs/constructs/`. Each page mixes hand-written prose about what its
constructs are *for* with a generated block of rows, keys and row-ID stability.
The prose is edited by hand; the generated blocks must not be.

A catalogue that disagrees with the manifest is worse than one that says nothing,
because a reader has no way to tell which half is current. This test makes that
drift a failure rather than a discovery.

Needs no database: rendering reads the manifest, not the registry.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from omop_constructs.core.catalogue import (
    BEGIN_MARKER,
    END_MARKER,
    INDEX_PAGE,
    family_page,
    render,
    render_pages,
    splice,
    sync_catalogue,
)
from omop_constructs.core.contracts import get_contracts
from omop_constructs.core.errors import ConstructSpecError

DOCS = Path(__file__).resolve().parents[1] / "docs"
CATALOGUE = DOCS / INDEX_PAGE


def test_catalogue_pages_are_current():
    stale = sync_catalogue(CATALOGUE, write=False)

    if stale:
        pytest.fail(
            "The construct catalogue is out of date with construct-contracts.toml: "
            f"{[str(page.relative_to(DOCS)) for page in stale]}. Regenerate it with:\n"
            "  python -m omop_constructs.core.catalogue docs/construct-catalog.md"
        )


@pytest.mark.parametrize("page", sorted(render_pages()))
def test_each_page_has_exactly_one_generated_block(page):
    text = (DOCS / page).read_text(encoding="utf-8")
    assert text.count(BEGIN_MARKER) == 1
    assert text.count(END_MARKER) == 1


def test_every_construct_has_a_card_on_its_family_page():
    """A construct absent from the rendering is a construct nobody can look up."""
    pages = render_pages()
    missing = [
        contract.name
        for contract in get_contracts()
        if f"{{#contract-{contract.name}}}" not in pages[family_page(contract.family)]
    ]
    assert not missing, f"constructs without a card: {missing}"


def test_every_construct_link_resolves_to_a_card():
    pages = render_pages()
    anchors = {
        (page, anchor)
        for page, block in pages.items()
        for anchor in re.findall(r"\{#(contract-[\w-]+)\}", block)
    }
    links = set()
    for page, block in pages.items():
        for target, anchor in re.findall(r"\]\(([\w/.-]*)#(contract-[\w-]+)\)", block):
            links.add((target or page, anchor))

    assert links, "expected construct links in the rendered pages"
    assert links <= anchors, f"links without a card: {sorted(links - anchors)}"


def test_splice_appends_when_no_marker_is_present():
    spliced = splice("# Some document\n\nProse.\n", render())
    assert spliced.startswith("# Some document")
    assert BEGIN_MARKER in spliced
    assert END_MARKER in spliced


def test_splice_replaces_an_existing_block_without_touching_the_prose():
    document = (
        "# Head\n\nKeep this.\n\n"
        f"{BEGIN_MARKER}\nstale content\n{END_MARKER}\n\nKeep this too.\n"
    )
    spliced = splice(document, render())

    assert "stale content" not in spliced
    assert "Keep this." in spliced
    assert "Keep this too." in spliced
    assert spliced.count(BEGIN_MARKER) == 1


def test_splice_refuses_an_unterminated_block():
    """Guessing where a block ends could silently delete hand-written prose."""
    with pytest.raises(ConstructSpecError, match="no .*END GENERATED"):
        splice(f"# Head\n\n{BEGIN_MARKER}\nunterminated\n", render())
