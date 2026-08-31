"""
Typed access to the D3 construct contract manifest.

`construct-contracts.toml` at the repository root records, for every registered
construct, its grain, its intended logical key, whether that key is complete,
whether any exposed surrogate is stable across refreshes, the expected fan-out
of each input join, concurrent-refresh eligibility, 1.0 inclusion, and lung
report usage.

The manifest rather than the ORM classes holds this because only part of it is
intrinsic. Grain and logical key belong to the construct; 1.0 inclusion and
lung-report usage are release and catalogue policy that would otherwise leak
into runtime classes. Keeping all of it in one file gives the coverage tests,
the release validation scripts, and the rendered catalogue a single source.

Declared keys are *intended* keys. Where a construct does not satisfy its
declared key today, `known_violations` names the finding. Callers that need to
know what the data actually looks like must measure it — see
`scripts/release_validation/` — rather than trusting the declaration.

The manifest is shipped inside the wheel so the public catalogue CLI works from
an installed package. An explicit path or ``OMOP_CONSTRUCTS_CONTRACTS`` can
still select a reviewed manifest outside the package.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator, Mapping

from .errors import ConstructSpecError

CONTRACTS_FILENAME = "construct-contracts.toml"
CONTRACTS_PATH_ENV = "OMOP_CONSTRUCTS_CONTRACTS"

FAN_OUT_KINDS = frozenset(
    {
        "one_to_one",
        "one_to_zero_or_one",
        "one_to_many",
        "many_to_one",
        "inner_lookup",
        "aggregate",
        "filter",
        "union_all",
    }
)
SURROGATE_KINDS = frozenset(
    {"source_identifier", "refresh_local_row_number", "none"}
)
LUNG_REPORT_ROLES = frozenset({"direct", "dependency", "unused"})

#: A ``construct``-scoped finding must be cited by at least one contract's
#: ``known_violations``. A ``package``-scoped one describes the package as a
#: whole — schema lifecycle, import-time I/O, mapper registration — and has no
#: single construct to attach to.
FINDING_SCOPES = frozenset({"construct", "package"})


@dataclass(frozen=True)
class ConstructInput:
    """
    One input join of a construct, and what it does to the row count.

    ``fan_out`` is the reviewed expectation, not a measurement. ``inner_lookup``
    is called out separately from ``many_to_one`` because the two compile to the
    same shape of join but only one of them can silently drop rows.
    """

    name: str
    kind: str
    fan_out: str
    note: str | None = None

    @property
    def multiplies(self) -> bool:
        """True when this join can increase the output row count."""
        return self.fan_out in {"one_to_many", "union_all"}

    @property
    def drops_rows(self) -> bool:
        """True when this join can silently remove input rows."""
        return self.fan_out in {"inner_lookup", "filter", "aggregate"}


@dataclass(frozen=True)
class ConstructContract:
    """
    The complete D3 contract for one construct.

    ``logical_key`` is the key the construct must satisfy to enter a 1.0
    release candidate. ``logical_key_complete`` is False when the columns needed
    to express the real grain are not projected at all, which is a stronger
    problem than a key that is merely violated by the current query.
    """

    name: str
    class_name: str
    module: str
    family: str
    grain: str
    logical_key: tuple[str, ...]
    logical_key_complete: bool
    key_nullable_columns: tuple[str, ...]
    unique_index_columns: tuple[str, ...]
    orm_primary_key: tuple[str, ...]
    surrogate_kind: str
    surrogate_stable_across_refresh: bool
    concurrent_refresh_eligible: bool
    public_api_1_0: bool
    lung_report_role: str
    oa_cohorts_rule_targets: tuple[str, ...]
    inputs: tuple[ConstructInput, ...]
    known_violations: tuple[str, ...] = ()
    concurrent_refresh_note: str | None = None
    public_api_note: str | None = None
    fan_out_note: str | None = None
    violation_note: str | None = None

    @property
    def satisfies_declared_key(self) -> bool:
        """
        Whether the declared key is expected to hold against real data today.

        This is the reviewed expectation represented by concurrent-refresh
        eligibility. Findings also cover non-key concerns such as unstable
        labels and schema lifecycle, so their presence alone is not evidence of
        a key violation. Use the uniqueness runner to establish the measured
        result against a particular CDM.
        """
        return self.logical_key_complete and self.concurrent_refresh_eligible

    @property
    def requires_nulls_not_distinct(self) -> bool:
        """
        Whether a unique index over the key needs NULLS NOT DISTINCT.

        A construct that keeps a null spine row — surgery, the episode
        hierarchies — has a genuine key containing a nullable column, and
        PostgreSQL's default UNIQUE treats those rows as always distinct.
        """
        return bool(set(self.key_nullable_columns) & set(self.unique_index_columns))

    @property
    def multiplying_inputs(self) -> tuple[ConstructInput, ...]:
        return tuple(i for i in self.inputs if i.multiplies)

    @property
    def row_dropping_inputs(self) -> tuple[ConstructInput, ...]:
        return tuple(i for i in self.inputs if i.drops_rows)


@dataclass(frozen=True)
class Finding:
    """One entry in the manifest's findings register."""

    finding_id: str
    scope: str
    severity: str
    origin: str
    summary: str
    detail: str | None = None


@dataclass(frozen=True)
class ContractManifest:
    """The parsed manifest: metadata, findings register, and contracts."""

    meta: Mapping[str, Any]
    findings: Mapping[str, Finding]
    contracts: Mapping[str, ConstructContract]
    path: Path = field(compare=False, default=Path())

    def __iter__(self) -> Iterator[ConstructContract]:
        return iter(self.contracts.values())

    def __len__(self) -> int:
        return len(self.contracts)

    def get(self, name: str) -> ConstructContract:
        try:
            return self.contracts[name]
        except KeyError:
            raise ConstructSpecError(
                f"{self.path.name} has no contract for construct '{name}'"
            ) from None

    def public_1_0(self) -> tuple[ConstructContract, ...]:
        return tuple(c for c in self if c.public_api_1_0)

    def excluded_from_1_0(self) -> tuple[ConstructContract, ...]:
        return tuple(c for c in self if not c.public_api_1_0)

    def lung_report_constructs(self) -> tuple[ConstructContract, ...]:
        """Constructs the lung report reaches, directly or through a dependency."""
        return tuple(c for c in self if c.lung_report_role != "unused")

    def with_violation(self, finding_id: str) -> tuple[ConstructContract, ...]:
        return tuple(c for c in self if finding_id in c.known_violations)

    def construct_scoped_findings(self) -> tuple[Finding, ...]:
        return tuple(f for f in self.findings.values() if f.scope == "construct")


def find_contracts_path(start: Path | None = None) -> Path:
    """
    Locate the manifest.

    Resolution order is an explicit ``OMOP_CONSTRUCTS_CONTRACTS`` override, then
    a walk up from ``start`` (this module by default) looking for the file. The
    walk is what lets tests, scripts, and the docs build all find it without
    each one hard-coding a relative path.
    """
    override = os.environ.get(CONTRACTS_PATH_ENV)
    if override:
        path = Path(override).expanduser()
        if not path.is_file():
            raise ConstructSpecError(
                f"{CONTRACTS_PATH_ENV} points at {path}, which is not a file"
            )
        return path

    origin = (start or Path(__file__)).resolve()
    for candidate in (origin, *origin.parents):
        path = candidate / CONTRACTS_FILENAME
        if path.is_file():
            return path

    raise ConstructSpecError(
        f"Could not find {CONTRACTS_FILENAME} above {origin}. Pass an explicit "
        f"path or set {CONTRACTS_PATH_ENV}; installed distributions should "
        "include the default manifest."
    )


def _require(table: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in table:
        raise ConstructSpecError(f"{where}: missing required field '{key}'")
    return table[key]


def _parse_input(raw: Mapping[str, Any], where: str) -> ConstructInput:
    fan_out = _require(raw, "fan_out", where)
    if fan_out not in FAN_OUT_KINDS:
        raise ConstructSpecError(
            f"{where}: unknown fan_out '{fan_out}'; expected one of {sorted(FAN_OUT_KINDS)}"
        )
    return ConstructInput(
        name=_require(raw, "name", where),
        kind=_require(raw, "kind", where),
        fan_out=fan_out,
        note=raw.get("note"),
    )


def _parse_contract(name: str, raw: Mapping[str, Any]) -> ConstructContract:
    where = f"constructs.{name}"

    surrogate_kind = _require(raw, "surrogate_kind", where)
    if surrogate_kind not in SURROGATE_KINDS:
        raise ConstructSpecError(
            f"{where}: unknown surrogate_kind '{surrogate_kind}'; "
            f"expected one of {sorted(SURROGATE_KINDS)}"
        )

    lung_report_role = _require(raw, "lung_report_role", where)
    if lung_report_role not in LUNG_REPORT_ROLES:
        raise ConstructSpecError(
            f"{where}: unknown lung_report_role '{lung_report_role}'; "
            f"expected one of {sorted(LUNG_REPORT_ROLES)}"
        )

    logical_key = tuple(_require(raw, "logical_key", where))
    if not logical_key:
        raise ConstructSpecError(f"{where}: logical_key must not be empty")

    nullable = tuple(raw.get("key_nullable_columns", ()))
    stray = sorted(set(nullable) - set(logical_key))
    if stray:
        raise ConstructSpecError(
            f"{where}: key_nullable_columns {stray} are not part of logical_key"
        )

    inputs = tuple(
        _parse_input(item, f"{where}.inputs[{i}]")
        for i, item in enumerate(_require(raw, "inputs", where))
    )
    if not inputs:
        raise ConstructSpecError(f"{where}: inputs must not be empty")

    return ConstructContract(
        name=name,
        class_name=_require(raw, "class_name", where),
        module=_require(raw, "module", where),
        family=_require(raw, "family", where),
        grain=_require(raw, "grain", where),
        logical_key=logical_key,
        logical_key_complete=bool(_require(raw, "logical_key_complete", where)),
        key_nullable_columns=nullable,
        unique_index_columns=tuple(_require(raw, "unique_index_columns", where)),
        orm_primary_key=tuple(_require(raw, "orm_primary_key", where)),
        surrogate_kind=surrogate_kind,
        surrogate_stable_across_refresh=bool(
            _require(raw, "surrogate_stable_across_refresh", where)
        ),
        concurrent_refresh_eligible=bool(
            _require(raw, "concurrent_refresh_eligible", where)
        ),
        public_api_1_0=bool(_require(raw, "public_api_1_0", where)),
        lung_report_role=lung_report_role,
        oa_cohorts_rule_targets=tuple(raw.get("oa_cohorts_rule_targets", ())),
        inputs=inputs,
        known_violations=tuple(raw.get("known_violations", ())),
        concurrent_refresh_note=raw.get("concurrent_refresh_note"),
        public_api_note=raw.get("public_api_note"),
        fan_out_note=raw.get("fan_out_note"),
        violation_note=raw.get("violation_note"),
    )


def _parse_finding(finding_id: str, raw: Mapping[str, Any]) -> Finding:
    where = f"findings.{finding_id}"
    scope = _require(raw, "scope", where)
    if scope not in FINDING_SCOPES:
        raise ConstructSpecError(
            f"{where}: unknown scope '{scope}'; expected one of {sorted(FINDING_SCOPES)}"
        )
    return Finding(
        finding_id=finding_id,
        scope=scope,
        severity=_require(raw, "severity", where),
        origin=_require(raw, "origin", where),
        summary=_require(raw, "summary", where),
        detail=raw.get("detail"),
    )


def load_contracts(path: str | Path | None = None) -> ContractManifest:
    """
    Parse and validate the manifest.

    Validation here is structural only: enumerations, required fields, and
    internal consistency between a construct's key and its nullable columns.
    Whether the manifest agrees with the registry is a separate check, because
    that needs the constructs imported — see
    ``tests/test_construct_contracts.py``.
    """
    resolved = Path(path) if path is not None else find_contracts_path()
    try:
        raw = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConstructSpecError(f"{resolved}: invalid TOML ({exc})") from exc

    findings = {
        finding_id: _parse_finding(finding_id, body)
        for finding_id, body in raw.get("findings", {}).items()
    }

    contracts = {
        name: _parse_contract(name, body)
        for name, body in raw.get("constructs", {}).items()
    }
    if not contracts:
        raise ConstructSpecError(f"{resolved}: no [constructs.*] tables found")

    unknown = sorted(
        {
            finding_id
            for contract in contracts.values()
            for finding_id in contract.known_violations
            if finding_id not in findings
        }
    )
    if unknown:
        raise ConstructSpecError(
            f"{resolved}: known_violations reference undefined findings {unknown}"
        )

    return ContractManifest(
        meta=raw.get("meta", {}),
        findings=findings,
        contracts=contracts,
        path=resolved,
    )


@lru_cache(maxsize=None)
def get_contracts() -> ContractManifest:
    """
    Return the manifest from its default location, parsed once per process.

    Cached because the coverage tests, the catalogue renderer, and the
    validation scripts all read it repeatedly and it never changes at runtime.
    """
    return load_contracts()
