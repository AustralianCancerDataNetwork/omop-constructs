# Release validation scripts

Transitional tooling for comparing two released versions of `omop-constructs`
against the same CDM before a result-changing release replaces a deployed
materialized view.

These scripts live outside the installed package on purpose. They orchestrate
two *different* installed versions of the library at once, which is not
something the library itself should be able to do. They are expected to be
deleted once OC-3 gives the registry schema-qualified DDL and a supported
comparison entry point.

## Requirements

- PostgreSQL. Materialized views, `pg_get_viewdef`, and `EXCEPT ALL` are all
  used directly.
- `uv` on `PATH`, to build the isolated side environments.
- A reachable CDM. Resolution order matches the test suite: `--cdm-url`, then
  the configured `cdm_db` resource, then `ENGINE_CDM` / `ENGINE`.

## Output classes

Every script writes into two subdirectories of `--output-dir`:

| Directory | Contents | Disclosure |
|---|---|---|
| `metadata/` | Row counts, definition checksums, duplicate counts, version numbers, generated SQL | Non-clinical; intended for review outside the secure environment |
| `clinical/` | Differing rows, affected person identifiers | Clinical; never leaves the secure environment, never committed |

`clinical/` is only created when a script is asked for row-level output, and
writing it into the repository working tree is refused outright. That check
exists because a misdirected `--output-dir` is the most plausible route to
patient data ending up somewhere it should not be.

## Workflow

### 1. Build both sides

```bash
python scripts/release_validation/build_side_schema.py \
  --baseline 'omop-constructs==0.7.0' \
  --baseline 'omop-alchemy[postgres]==1.1.0' \
  --candidate . \
  --candidate ../omop-alchemy/dist/omop_alchemy-1.1.1.dev5+ge7737e1c1-py3-none-any.whl \
  --schema-prefix oc_transition \
  --with-data \
  --output-dir /secure/oc0-run
```

Each `--baseline` / `--candidate` is repeatable and takes wheel paths, source
directories, or PyPI pins, so **each side carries its own `omop-alchemy`**. That
is what makes the comparison honest: nothing is shared between the two
environments except the CDM.

The selected `--cdm-url` is also forced into the environment before either
version imports its construct modules. This matters because concept resolvers
run during import: allowing an operator's otherwise valid stack config to win
there could resolve concepts from one CDM while the build engine writes views
over another.

Views are placed with `search_path = <side schema>, <cdm schema>`. The registry
emits unqualified DDL, so each side's views are created in its own schema while
CDM inputs resolve from the CDM schema and inter-construct references resolve
within the side. This deliberately exploits the schema-dependence that OC-B5
identifies as a defect — which is the reason this script is transitional.

Without `--with-data` the build is structural only. That is enough to compare
definitions and column sets, and it touches no patient data, so it is the right
first run against an unfamiliar database.

Writes `metadata/side_schema_build.json`: installed versions per side, the build
plan, per-construct compiled-SQL checksums, any build failures, and the
structural diff between the two sides. A construct whose compiled checksum is
unchanged cannot have changed its results, so that list is where to start.

### 2. Record the uniqueness and size baseline

```bash
python scripts/release_validation/collect_key_metrics.py \
  --schema oc_transition_candidate --label candidate --output-dir /secure/oc0-run
python scripts/release_validation/collect_key_metrics.py \
  --schema oc_transition_baseline --label baseline --output-dir /secure/oc0-run
```

For each construct in `construct-contracts.toml`: the deployed definition
checksum, row count, rows with a NULL anywhere in the declared logical key,
distinct logical-key count, and the resulting duplicate count.

Declared keys in the manifest are *intended* keys. At OC-0 most constructs are
expected to report duplicates — that is the baseline evidence, not a failure, so
the script exits zero. `--fail-on-duplicates` turns it into a release gate once
OC-2 has landed.

Uniqueness is measured as `count(*) = count(distinct (k1, ..., kn))`, which is
stricter than a unique index: `count(distinct)` on a row value treats NULLs as
equal, so a construct with a null spine cannot hide duplicate spine rows behind
PostgreSQL's default NULL-distinct behaviour.

### 3. Compare the two sides

```bash
python scripts/release_validation/compare_schemas.py \
  --baseline-schema oc_transition_baseline \
  --candidate-schema oc_transition_candidate \
  --output-dir /secure/oc0-run \
  --emit-rows
```

When both sides expose the same column set, compares them with `EXCEPT ALL` in
both directions, then summarises by construct and — where the construct exposes
`person_id` — by person. A changed column set is non-comparable and is never
treated as a match over only the shared subset.

`EXCEPT ALL` rather than `EXCEPT` because plain `EXCEPT` deduplicates, which
would hide a multiplicity change: the same distinct rows returned three times
instead of once. That is the OC-H1 and OC-B2 failure mode, so it is the thing
most worth detecting.

`mv_id` is excluded from every comparison. It is an unordered `row_number()` and
differs between any two builds by construction, so including it would mark every
row of every construct as changed.

Columns present in only one side are reported rather than quietly dropped: a
construct whose column set changed cannot be compared column-for-column, and
comparing a subset without saying so would misreport a schema change as a data
match.

Exact difference and affected-person counts are computed inside PostgreSQL.
Without `--emit-rows`, no row payload or person identifier is transferred to
Python. With it, each query is bounded in PostgreSQL by
`--max-rows-per-construct` (default 100,000); metadata records the exact total,
the number written, and every truncation.

`metadata/schema_comparison.sql` holds the generated SQL, so an operator can run
and audit the comparison by hand without trusting this script.

Useful narrowing: `--lung-only` restricts to constructs the pre-production lung
report reaches; `--only-public` restricts to the 1.0 public surface.

### 4. Clean up

The side schemas hold populated copies of every construct. Drop them when the
comparison has been reviewed:

```sql
DROP SCHEMA oc_transition_baseline CASCADE;
DROP SCHEMA oc_transition_candidate CASCADE;
```

Re-running `build_side_schema.py` over an existing pair requires
`--replace-schemas`, so a stale build is never silently compared.

## OC-0 evidence gate

The repository-side part of OC-0 is complete when its tests and static checks
pass and the default contract manifest is available from an installed wheel.
Closing OC-0 additionally requires one authorised, populated-CDM run whose
non-clinical evidence retains all of the following together:

- `side_schema_build.json` for the exact baseline and candidate dependency
  sets, with both sides successfully built;
- baseline and candidate `key_metrics_*.csv` and `key_metrics_*.json` files for
  every selected construct;
- `schema_comparison.csv`, `schema_comparison.json`, and the generated SQL,
  with every changed-column construct explicitly reviewed as non-comparable;
- reviewer disposition for every definition or row difference, including the
  lung-report impact audit where applicable.

Row samples and person-level files remain in the secure environment. Their
metadata totals and reviewer disposition are the retainable release evidence.

## Reading a comparison

1. `metadata/side_schema_build.json` → `definition_diff.definition_changed`.
   Only these constructs can have changed results. If a construct outside this
   list shows row differences, the two builds did not see the same CDM.
2. `metadata/schema_comparison.csv` → `rows_only_in_baseline` and
   `rows_only_in_candidate` per construct. Equal non-zero counts with a
   `net_row_change` of zero mean rows changed shape rather than appearing or
   disappearing.
3. `clinical/<construct>__persons.csv` → whether a delta touches a handful of
   patients or the whole cohort. A change concentrated in a few people is a data
   finding; one spread evenly is a query change.
4. For anything in the lung report, cross-check against
   `lung-oc-b1-b2-clinical-impact-audit.sql`.

## Testing

The SQL these scripts generate is covered by
`tests/test_release_validation_sql.py`, which builds synthetic views with
hand-computed expected numbers. Run it before trusting a comparison:

```bash
python -m pytest tests/test_release_validation_sql.py -m postgres -q
```
