# OMOP Constructs

`omop-constructs` is the analytical layer that sits between raw OMOP CDM models and downstream cohort or reporting code.

It builds reusable SQLAlchemy queries and materialized views on top of:

- `omop-alchemy` for typed OMOP table models
- `omop-semantics` for concept groups and runtime value sets

In this repository, the main patterns in active use are:

- episode-derived cancer constructs
- diagnosis-linked observations, procedures, measurements, and visits
- treatment summaries and treatment window scalars
- staging and modifier resolution
- registry-driven materialized view lifecycle management

## What Is In Scope Today

The current library is centered on oncology-style episode linkage. The most important construct families are:

- `omop_constructs.alchemy.modifiers`
  Stage and modifier materialized views, then episode-linked condition joins such as `ModifiedCondition`.
- `omop_constructs.alchemy.episodes`
  Disease episodes, treatment episodes, treatment envelopes, intent summaries, and consult windows.
- `omop_constructs.alchemy.events`
  Diagnosis-linked measurements, procedures, observations, and specialist/provider visits.
- `omop_constructs.alchemy.demography`
  Person-level demographics attached to condition episodes.
- `omop_constructs.core`
  Registration, dependency planning, and materialized view DDL helpers.
- `omop_constructs.semantics`
  Runtime concept resolver registry used by the staging and modifier layer.

## Current Usage Pattern

The library is used in two complementary ways.

### 1. Direct SQLAlchemy composition

You can import query fragments or mapped materialized view classes and build further SQL on top of them.

```python
from omop_constructs.alchemy.episodes import TreatmentEnvelopeMV, ConsultWindowMV

stmt = (
    TreatmentEnvelopeMV.__mv_select__
    .where(TreatmentEnvelopeMV.days_from_dx_to_treatment > 30)
)
```

### 2. Registry-driven materialized view management

Construct classes register themselves when their modules are imported. Once the relevant construct families are imported, `ConstructRegistry` can plan, create, refresh, inspect, or validate the registered materialized views.

```python
from omop_constructs.alchemy import events, episodes, modifiers, demography  # noqa: F401
from omop_constructs.core import get_construct_registry

registry = get_construct_registry()
plan = registry.plan()
```

## Important Runtime Notes

- Registration is import-driven.
  `get_construct_registry()` only sees construct classes from modules that have already been imported in the current process.
- The modifier layer is semantics-backed.
  Importing parts of `omop_constructs.alchemy.modifiers` can trigger runtime resolver setup through `omop_constructs.semantics`.
- Materialized view lifecycle helpers are PostgreSQL-oriented.
  `ConstructRegistry` uses `pg_matviews` for existence checks and emits `CREATE MATERIALIZED VIEW` / `REFRESH MATERIALIZED VIEW` DDL.

## Documentation Map

- [Usage](usage.md): import patterns, registry workflow, and operational notes
- [Architecture](architecture.md): layering, registration, dependency planning, and event attachment
- [Construct Catalog](construct-catalog.md): current construct families and the materialized views they expose

## Recent Additions Reflected Here

The docs now cover the current consult and specialist linkage path:

- `DxRelevantVisitMV` for episode-linked provider-specialty visits
- `ConsultWindowMV` for referral-to-specialist and referral-to-treatment window scalars

These are now part of the active public construct surface alongside the older treatment and diagnosis-linked event materialized views.
