"""Keep the published catalogue in step with the contract manifest.

`docs/construct-catalog.md` mixes hand-written prose about what each construct is
*for* with a generated block holding grains, keys, the 1.0 surface, and the
findings register. The prose is edited by hand; the generated block must not be.

A catalogue that disagrees with the manifest is worse than one that says nothing,
because a reader has no way to tell which half is current. This test makes that
drift a failure rather than a discovery.

Needs no database: rendering reads the manifest, not the registry.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omop_constructs.core.catalogue import (
    BEGIN_MARKER,
    END_MARKER,
    render,
    splice,
)
from omop_constructs.core.contracts import get_contracts
from omop_constructs.core.errors import ConstructSpecError

CATALOGUE = Path(__file__).resolve().parents[1] / "docs" / "construct-catalog.md"


def test_catalogue_generated_block_is_current():
    current = CATALOGUE.read_text(encoding="utf-8")
    expected = splice(current, render())

    if current != expected:
        pytest.fail(
            "docs/construct-catalog.md is out of date with construct-contracts.toml. "
            "Regenerate it with:\n"
            "  python -m omop_constructs.core.catalogue docs/construct-catalog.md"
        )


def test_catalogue_has_exactly_one_generated_block():
    text = CATALOGUE.read_text(encoding="utf-8")
    assert text.count(BEGIN_MARKER) == 1
    assert text.count(END_MARKER) == 1


def test_every_construct_appears_in_the_generated_block():
    """A construct absent from the rendering is a construct nobody can look up."""
    block = render()
    missing = [contract.name for contract in get_contracts() if f"`{contract.name}`" not in block]
    assert not missing, f"constructs missing from the rendered catalogue: {missing}"


def test_every_finding_appears_in_the_generated_block():
    block = render()
    missing = [
        finding_id for finding_id in get_contracts().findings if f"`{finding_id}`" not in block
    ]
    assert not missing, f"findings missing from the rendered register: {missing}"


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
