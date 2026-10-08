# Release comparison tools

Tools for checking built construct schemas before a result-changing release
replaces a deployed materialized view. They read schemas from the outside and do
not care how a schema was built: a production schema, a side schema from a
validation pack, or a scratch build.

| Tool | Answers |
|---|---|
| `collect_key_metrics.py` | Does each construct in one schema satisfy its declared key in `construct-contracts.toml`? |
| `compare_schemas.py` | Which rows differ between two schemas, construct by construct and person by person? |

`_common.py` holds the shared database and output helpers. `_sql_normalise.py`
makes definition checksums ignore the order of embedded concept-ID lists, which
the package currently renders in hash-seed-dependent order (finding OC-0-N10);
it can be retired once the package renders those lists in sorted order.

The tools live outside the installed package on purpose. The SQL they generate
is tested against synthetic views with hand-computed answers in
`tests/test_release_validation_sql.py`.

## Requirements

- PostgreSQL. Materialized views, `pg_get_viewdef`, and `EXCEPT ALL` are used directly.
- A reachable CDM. Resolution order matches the test suite: `--cdm-url`, then the
  configured `cdm_db` resource, then `ENGINE_CDM` / `ENGINE`.

## Output classes

Every tool writes into two subdirectories of `--output-dir`:

| Directory | Contents | Disclosure |
|---|---|---|
| `metadata/` | Row counts, definition checksums, duplicate counts, version numbers, generated SQL | Non-clinical; can be reviewed outside the secure environment |
| `clinical/` | Differing rows, affected person identifiers | Clinical; never leaves the secure environment, never committed |

`clinical/` is created only when row-level output is requested, and writing it
inside the repository working tree is refused.

## Measure a schema against the contracts

```bash
uv run --no-sync python tools/release_validation/collect_key_metrics.py \
  --schema <schema> --label candidate --output-dir /secure/run
```

For each construct in `construct-contracts.toml`: the deployed definition
checksum, row count, rows with a NULL anywhere in the declared key, distinct key
count, and the resulting duplicate count. Declared keys are intended keys, so
duplicates are reported rather than failing the run; `--fail-on-duplicates`
turns the run into a gate.

Uniqueness is measured as `count(*) = count(distinct (k1, ..., kn))`. That is
stricter than a unique index: a construct with a NULL spine cannot hide
duplicate spine rows behind PostgreSQL's NULL-distinct behaviour.

## Compare two schemas

```bash
uv run --no-sync python tools/release_validation/compare_schemas.py \
  --baseline-schema <old schema> \
  --candidate-schema <new schema> \
  --output-dir /secure/run \
  --emit-rows
```

When both sides expose the same columns, the tool compares them with `EXCEPT
ALL` in both directions, then summarises by construct and, where the construct
has `person_id`, by person.

- `EXCEPT ALL`, not `EXCEPT`: plain `EXCEPT` deduplicates and would hide a row
  that now appears three times instead of once.
- `mv_id` is excluded: it is a refresh-local `row_number()` and differs between
  any two builds.
- A changed column set is reported as non-comparable, never matched over the
  shared columns only.
- Counts are exact and computed in PostgreSQL. Without `--emit-rows`, no row or
  person identifier leaves the database; with it, each query is bounded by
  `--max-rows-per-construct` (default 100,000) and every truncation is recorded.
- `metadata/schema_comparison.sql` holds the generated SQL, so the comparison
  can be audited and rerun by hand.

## Reading a comparison

1. `metadata/schema_comparison.csv`: `rows_only_in_baseline` and
   `rows_only_in_candidate` per construct. Equal non-zero counts with a
   `net_row_change` of zero mean rows changed shape rather than appearing or
   disappearing.
2. `clinical/<construct>__persons.csv`: whether a difference touches a few
   patients or the whole cohort. A change concentrated in a few people is a data
   finding; one spread evenly is a query change.
