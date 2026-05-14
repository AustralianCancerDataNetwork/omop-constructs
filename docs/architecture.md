# Architecture

## Layering

The repository is organized into a small set of layers.

### Core

`omop_constructs.core` handles:

- construct registration
- dependency planning
- materialized view DDL
- registry inspection and validation

This is the operational layer that turns mapped SQLAlchemy classes into something you can manage as a dependency graph.

### Semantics

`omop_constructs.semantics` provides runtime concept resolver setup on top of `omop-semantics` value sets and `omop-alchemy` resolver helpers.

The current default resolver set includes:

- `tnm_t_stage`
- `tnm_n_stage`
- `tnm_m_stage`
- `tnm_group_stage`
- `metastatic_disease`
- `tumor_grade`
- `rt_procedures`
- `country_of_birth`

### Alchemy Construct Families

The `omop_constructs.alchemy` package contains the construct definitions themselves:

- `modifiers`
- `episodes`
- `events`
- `demography`
- `conditions`

These are mostly SQLAlchemy `select()` fragments plus ORM-mapped materialized view classes.

## Registration Model

Construct classes are registered through `@register_construct`.

This has two consequences:

- registration happens at import time
- the registry is process-local and reflects what has been imported so far

That import-driven behavior is current, intentional usage and should be assumed by downstream consumers.

## Dependency Planning

`ConstructRegistry.plan()` builds a DAG from each construct class's `__deps__` tuple and topologically sorts it.

That ordering is reused for:

- `create_all`
- `refresh_all`
- `create_missing`
- `refresh_existing`

and reversed for `drop_all`.

Dependencies can point at constructs outside the currently imported set; those are treated as external and skipped during sorting rather than rejected.

## Event Attachment Strategy

The active event-linkage code centers on `omop_constructs.alchemy.events.event_factories`.

For procedures, measurements, and observations the library supports:

- explicit attachment via `Episode_Event`
- fallback time-window attachment to `ConditionEpisodeMV`
- post-filtering through `episode_relevant_window`

This keeps downstream event MVs consistent:

- person identifier
- event identifier
- event date
- event concept metadata
- attached disease episode metadata

## Visit Linkage

Visits are handled differently from observations and measurements.

`DxRelevantVisitMV` uses a ranked specialty-visit linking approach derived from the older specialist-visit construct lineage:

- prioritize visits close to the episode start
- otherwise prefer the nearest prior episode
- otherwise fall back to the closest episode by distance

This is why the consult-window path has a dedicated visit construct rather than reusing the generic event-factory windowing logic.

## Treatment Window And Consult Window Pattern

The episode layer currently exposes two important scalar-style constructs:

- `TreatmentEnvelopeMV`
  earliest/latest treatment and treatment-derived scalar windows
- `ConsultWindowMV`
  referral-derived specialist and treatment windows

`ConsultWindowMV` is built by combining:

- episode-of-care anchors
- diagnosis-linked consult observations
- episode-linked provider-specialty visits
- earliest treatment from `TreatmentEnvelopeMV`

## Current Design Bias

The current codebase is strongly biased toward:

- oncology use cases
- disease episode resolution
- materialized views as the main reusable output shape
- semantics-backed concept grouping instead of hard-coded concept lists in downstream consumers

The docs in this folder describe that current bias rather than a hypothetical generic construct framework.
