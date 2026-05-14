# Usage

## Prerequisites

Typical runtime usage assumes:

- `omop-alchemy` is installed and can import the OMOP CDM models
- `omop-semantics` runtime value sets are available
- `orm_loader.helpers.Base` resolves in the active environment
- a PostgreSQL database is available for materialized view creation and refresh

If you use the semantics-backed modifier layer, database-backed resolver setup must also be available at import time.

## Importing Construct Families

Constructs are registered via the `@register_construct` decorator when their modules are imported.

That means this:

```python
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()
```

is not enough on its own in a fresh process.

Import the families you want first:

```python
from omop_constructs.alchemy import events, episodes, modifiers, demography  # noqa: F401
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()
```

If you only need a subset, import only those modules. The registry will then contain only the imported construct classes.

## Inspecting The Registry

```python
from omop_constructs.alchemy import events, episodes, modifiers, demography  # noqa: F401
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()

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

## Current Semantics-Driven Behavior

The modifier and staging layer depends on runtime concept resolvers from `omop_constructs.semantics`.

In practice this means:

- importing `omop_constructs.alchemy.modifiers` is not a pure no-op
- stage and modifier query fragments can depend on resolver-backed concept expansion
- environment loading for the resolver engine may happen if `ENGINE` is not already configured

If you want a lighter import path for event or episode constructs only, avoid importing modifier modules unless you need them.

## Current Episode-Linkage Patterns

The codebase currently uses three main episode-linkage strategies:

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
  import the construct families before calling `get_construct_registry()`
- Import-time resolver errors:
  check semantics runtime configuration before importing modifier-heavy modules
- Schema mismatch during validation:
  the mapped class and underlying materialized view definition have drifted
- Missing construct in downstream code:
  confirm the module containing that construct class has actually been imported
