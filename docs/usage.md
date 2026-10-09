# Usage

## Prerequisites

Typical runtime usage assumes:

- `oa-configurator` is installed and a shared OMOP stack config is available
- `omop-alchemy>=1.2.2.dev0,<2` (the local Q4 wheel selected by `tool.uv.sources`), `omop-semantics`, and `orm-loader>=1.2.0,<2` are installed
- `omop-semantics` runtime value sets are available
- a PostgreSQL database is available for materialized view creation and refresh

The modifier layer builds database-side concept predicates without opening a connection at import time. Other semantics-backed construct families still require database-backed resolver setup.

Diagnosis-linked measurements, procedures, and observations preserve every valid explicit episode link. An event without an explicit link attaches to one episode of care: an episode already started at the event date is preferred, followed by the nearest episode start and then the lowest `episode_id`. When the nearest eligible already-started episode began at least 365 days before the event, choose the earliest eligible upcoming episode starting within 60 days after the event, breaking equal starts by the lowest episode ID. Both thresholds are inclusive. A more recent started episode blocks this override, and a same-day start is already started. A valid explicit relationship to the second episode takes precedence over this date-based choice. Progression and metastatic episodes are never chosen by date; they receive events only through valid explicit links. Custom queries can override the default with explicit `policy` and `ranking` arguments.

## Configuration With `omop-config`

`omop-constructs` reads its CDM resource and logging settings through `oa-configurator`.

Set up the shared stack configuration with:

```bash
omop-config init
omop-config configure omop_alchemy
omop-config configure omop_constructs
```

`omop-config configure omop_constructs` is the package entry point registered under `omop.config`. It validates that a `cdm_db` resource is available and can store a package-specific `default_resource` when `omop-constructs` should use a different CDM resource than `omop-alchemy`.

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

- `registry.describe()`: human-readable dependency overview
- `registry.plan()`: dependency-sorted construct plan
- `registry.build_plan_json()`: JSON-serializable version of the plan
- `registry.explain(bind)`: SQL length plus existence and optional row counts
- `registry.validate(bind)`: database schema vs mapper comparison

## Creating And Refreshing Materialized Views

```python
registry.create_all(engine)
registry.refresh_all(engine)
```

Safer operational variants are also available:

- `registry.create_missing(bind)`: only create views that do not yet exist
- `registry.refresh_existing(bind)`: only refresh views that already exist

These helpers assume PostgreSQL materialized views and use `pg_matviews` for existence checks.

### Deploying event-attachment definition changes

The diagnosis-linked event constructs use an explicit-first attachment policy. A valid `Episode_Event` relationship must match the event ID, Field-concept discriminator, and person. It is emitted once and suppresses date-window fallback for that table-scoped event. An event without a valid explicit relationship is attached to one eligible episode of care using the deterministic ranking described above.

The default lower bound is the episode-of-care start minus 90 days. Its upper bound is the greatest of its own window end and the latest window end of its same-person nested progression or metastatic diagnoses. Each episode's own window end is its recorded end date, or its start plus 365 days when the end is absent. Extension changes fallback eligibility only: explicit links, root-start ranking, and recorded episode dates in the output remain unchanged. Custom `EpisodeWindowSpec` horizons and bound inclusivity also apply to the extended candidate. All-in-window and explicit-only policies retain their existing behaviour. 

The attachment policy is part of each materialized-view definition. PostgreSQL `REFRESH MATERIALIZED VIEW` repopulates the definition already stored in the database, so installing a release with a changed attachment policy requires a rebuild. The affected definitions are:

- `dx_measurement_mv`, `dx_observation_mv`, and `dx_procedure_mv`
- the concept-specific measurement views (`weight_dx_mv`, `weight_change_dx_mv`, `height_dx_mv`, `bsa_dx_mv`, `creatinine_clearance_dx_mv`, `egfr_dx_mv`, `fev1_dx_mv`, `dtherm_dx_mv`, `ecog_dx_mv`, and `smoking_pyh_dx_mv`)
- `consult_window_mv`, which depends on `dx_observation_mv`.


For a registry-managed deployment, rebuild the managed construct set in dependency order:

```python
from omop_constructs.bootstrap import get_complete_construct_registry

registry = get_complete_construct_registry()

with engine.begin() as connection:
    registry.drop_all(connection)
    registry.create_all(connection)
```

Validate the rebuilt views in a side schema before replacing a populated clinical deployment. A selective rebuild is also possible, but the operator must drop downstream views before their inputs and recreate inputs before downstream views; the registry does not expose a selective cascade planner.

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
- resolver-backed imports require a resolvable CDM resource in the active `oa-configurator` stack config

If you want a lighter import path for event or episode constructs only, avoid importing modifier modules unless you need them.

## Episode-Linkage Patterns

The codebase uses three main episode-linkage strategies:

- explicit-first `Episode_Event` linkage for observations, procedures, and measurements, with date-based fallback only when no valid explicit relationship exists
- single-episode ranked attachment to an episode of care in `ConditionEpisodeMV` for unlinked observations, procedures, and measurements
- specialty-specific visit ranking for `DxRelevantVisitMV`

For example, the first two strategies ensure that a spirometry measurement explicitly recorded against two lung cancer episodes remains visible in both, while an unlinked spirometry measurement that merely falls within both date windows is assigned to one episode. The consult-window path combines:

- `DxObservationMV`
- `DxRelevantVisitMV`
- `TreatmentEnvelopeMV`

Together, these views compute episode-level referral-to-specialist and referral-to-treatment windows.

## Common Pitfalls

- Empty registry after startup: use `get_complete_construct_registry()` or import the construct families before calling `get_construct_registry()`.
- Import-time resolver errors: check the active `oa-configurator` stack config before importing modifier-heavy modules.
- Schema mismatch during validation: the mapped class and underlying materialized view definition have drifted.
- Missing construct in downstream code: confirm the module containing that construct class has actually been imported.


Ranked attachment queries carry a private `_fallback_window_end` alongside `attachment_method`. `episode_relevant_window` consumes both, so its final filter honours the extension while materialized views retain recorded episode dates and their existing columns. Configure ranked open-end horizons with `EpisodeWindowSpec` at attachment time. All-in-window and explicit-only queries keep their original window path.
