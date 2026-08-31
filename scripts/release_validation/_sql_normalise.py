"""Normalise rendered SQL so a checksum reflects meaning, not list ordering.

Construct queries embed resolved concept-ID sets as literal ``IN (...)`` lists.
Those lists are rendered in Python set-iteration order, which depends on
``PYTHONHASHSEED``, so the same construct built twice by the same library version
produces different SQL text and therefore different checksums.

That matters because the release validation workflow uses "did the definition
change?" to decide which constructs can possibly have changed results. Without
normalisation, roughly a quarter of the registry reports as changed on every run
and the signal is worthless.

Sorting the members of each numeric ``IN`` list makes the checksum stable while
still changing whenever the *set* of concepts changes, which is the thing worth
detecting.

Deliberately free of third-party imports: this module is imported both by the
orchestrator and by the build driver running inside an isolated side
environment, which holds a different version of the stack.
"""

from __future__ import annotations

import hashlib
import re

#: An IN list whose members are all plain integers. Restricted to numeric
#: members on purpose: reordering a concept-ID list is meaning-preserving, but
#: reordering something these patterns do not recognise might not be, and
#: silently sorting it would hide a real change.
_NUMERIC_IN_LIST = re.compile(r"\bIN\s*\(\s*(\d+(?:\s*,\s*\d+)*)\s*\)", re.IGNORECASE)

#: The same list as PostgreSQL renders it back. ``pg_get_viewdef`` rewrites
#: ``x IN (1, 2, 3)`` as ``x = ANY (ARRAY[1, 2, 3])``, so normalising only the
#: ``IN`` form would leave every deployed definition unstable — which is exactly
#: the form the release comparison reads. Optional ``::type[]`` cast suffix
#: because PostgreSQL adds one for non-integer element types.
_NUMERIC_ANY_ARRAY = re.compile(
    r"=\s*ANY\s*\(\s*ARRAY\[\s*(\d+(?:\s*,\s*\d+)*)\s*\]"
    r"(\s*::\s*[a-z_][a-z0-9_ ]*\[\])?\s*\)",
    re.IGNORECASE,
)


def _sorted_members(raw: str) -> str:
    return ", ".join(str(member) for member in sorted(int(p) for p in raw.split(",")))


def normalise_sql(sql: str) -> str:
    """
    Return ``sql`` with the members of every numeric concept list sorted.

    Handles both the form SQLAlchemy renders (``IN (...)``) and the form
    PostgreSQL renders it back as (``= ANY (ARRAY[...])``), so a compiled select
    and a deployed ``pg_get_viewdef`` are each stabilised.
    """

    def _fix_in(match: re.Match[str]) -> str:
        return "IN (" + _sorted_members(match.group(1)) + ")"

    def _fix_any(match: re.Match[str]) -> str:
        cast = (match.group(2) or "").strip()
        return "= ANY (ARRAY[" + _sorted_members(match.group(1)) + "]" + cast + ")"

    return _NUMERIC_ANY_ARRAY.sub(_fix_any, _NUMERIC_IN_LIST.sub(_fix_in, sql))


def strip_schema_qualification(sql: str, schema: str) -> str:
    """
    Remove ``schema.`` prefixes for one schema from rendered SQL.

    PostgreSQL resolves and stores the qualified name of every object a view
    references, so ``pg_get_viewdef`` on a construct built into a side schema
    reports ``oc_baseline.condition_episode_mv`` while the same construct in the
    other side reports ``oc_candidate.condition_episode_mv``. Without stripping,
    every construct that reads another construct would checksum differently
    between the two sides purely because of where it was built — which is the
    one thing a side-by-side comparison must not be confused by.

    Only the named schema is stripped. The CDM schema stays qualified, so a
    candidate that started reading a different CDM schema still shows up as a
    change.
    """
    pattern = re.compile(rf'(?<![\w."]){re.escape(schema)}\.|"{re.escape(schema)}"\.')
    return pattern.sub("", sql)


def sql_checksum(sql: str, *, strip_schema: str | None = None) -> str:
    """md5 of the normalised SQL, optionally with one schema's prefixes removed."""
    normalised = normalise_sql(sql)
    if strip_schema:
        normalised = strip_schema_qualification(normalised, strip_schema)
    return hashlib.md5(normalised.encode("utf-8")).hexdigest()


def raw_checksum(sql: str) -> str:
    """
    md5 of the SQL exactly as rendered.

    Kept alongside the normalised checksum so a run can distinguish "the query
    changed" from "only the concept-list ordering changed", which is what an
    unstable raw checksum with a stable normalised one means.
    """
    return hashlib.md5(sql.encode("utf-8")).hexdigest()
