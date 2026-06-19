# Usage

## Prerequisites

Typical runtime usage assumes:

- `oa-configurator` is installed and a shared OMOP stack config is available
- `omop-alchemy`, `omop-semantics`, and `orm-loader` are installed
- `omop-semantics` runtime value sets are available
- a PostgreSQL database is available for materialized view creation and refresh

If you use the semantics-backed modifier layer, database-backed resolver setup must also be available at import time.

## Configuration With `omop-config`

`omop-constructs` reads its CDM resource and logging settings through
`oa-configurator`.

Set up the shared stack configuration with:

```bash
omop-config init
omop-config configure omop_alchemy
omop-config configure omop_constructs
```

`omop-config configure omop_constructs` is the package entry point registered
under `omop.config`. It validates that a `cdm_db` resource is available and can
store a package-specific `default_resource` when `omop-constructs` should use a
different CDM resource than `omop-alchemy`.

## Importing Construct Families

Constructs are registered via the `@register_construct` decorator when their modules are imported.

That means this:

```python
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()
```

is not enough on its own in a fresh process.

If you want the full construct registry, use the bootstrap helper:

```python
from omop_constructs.bootstrap import get_complete_construct_registry

registry = get_complete_construct_registry()
```

If you want only a subset, import the families you need first:

```python
from omop_constructs.alchemy import events, episodes, modifiers, demography  # noqa: F401
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()
```

If you only need a subset, import only those modules. The registry will then contain only the imported construct classes.

## Inspecting The Registry

```python
from omop_constructs.bootstrap import get_complete_construct_registry

registry = get_complete_construct_registry()

print(registry.describe())
print(registry.plan())
```

Useful inspection methods:

- `registry.describe()`
  Human-readable dependency overview
- `registry.plan()`
  Dependency-sorted construct plan
- `registry.build_plan_json()`
  JSON-serializable version of the plan
- `registry.explain(bind)`
  SQL length plus existence and optional row counts
- `registry.validate(bind)`
  Database schema vs mapper comparison

## Creating And Refreshing Materialized Views

```python
registry.create_all(engine)
registry.refresh_all(engine)
```

Safer operational variants are also available:

- `registry.create_missing(bind)`
  Only create views that do not yet exist
- `registry.refresh_existing(bind)`
  Only refresh views that already exist

These helpers assume PostgreSQL materialized views and use `pg_matviews` for existence checks.

## CLI

The package exposes a small command-line interface for registry artefacts:

```bash
omop-constructs schema-snapshot tests/artifacts/construct_registry_schema.csv
```

The same export is available as a module entry point:

```bash
python -m omop_constructs.core.schema_snapshot tests/artifacts/construct_registry_schema.csv
```

## Direct Use Of Materialized View Classes

Every registered construct class exposes its `__mv_select__` and mapped columns, so you can compose on top of them with SQLAlchemy.

```python
from omop_constructs.alchemy.episodes import TreatmentEnvelopeMV
import sqlalchemy as sa

stmt = (
    sa.select(
        TreatmentEnvelopeMV.person_id,
        TreatmentEnvelopeMV.condition_episode,
        TreatmentEnvelopeMV.days_from_dx_to_treatment,
    )
    .where(TreatmentEnvelopeMV.days_from_dx_to_treatment > 30)
)
```

## Semantics-Driven Behavior

The modifier and staging layer depends on runtime concept resolvers from `omop_constructs.semantics`.

In practice this means:

- importing `omop_constructs.alchemy.modifiers` is not a pure no-op
- stage and modifier query fragments can depend on resolver-backed concept expansion
- resolver-backed imports require a resolvable CDM resource in the active
  `oa-configurator` stack config

If you want a lighter import path for event or episode constructs only, avoid importing modifier modules unless you need them.

## Episode-Linkage Patterns

The codebase uses three main episode-linkage strategies:

- explicit `Episode_Event` linkage where available
- time-window attachment to `ConditionEpisodeMV` for observations, procedures, and measurements
- specialty-specific visit ranking for `DxRelevantVisitMV`

The consult-window path combines:

- `DxObservationMV`
- `DxRelevantVisitMV`
- `TreatmentEnvelopeMV`

to compute episode-level referral-to-specialist and referral-to-treatment windows.

## Common Pitfalls

- Empty registry after startup:
  use `get_complete_construct_registry()` or import the construct families before calling `get_construct_registry()`
- Import-time resolver errors:
  check the active `oa-configurator` stack config before importing modifier-heavy modules
- Schema mismatch during validation:
  the mapped class and underlying materialized view definition have drifted
- Missing construct in downstream code:
  confirm the module containing that construct class has actually been imported
