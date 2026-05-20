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

`DxRelevantVisitMV` exposes provider-specialty visits linked to disease episodes. 
It uses a ranked proximity approach rather than the generic event-factory time-window 
attachment path:

- visits within ±180 days of the episode start (`episode_prior == 1`) are always included
- for each visit, `rank=1` identifies its single highest-priority episode assignment,
  ordered by proximity tier then absolute day distance
- multiple visits per episode appear as separate rows
- each row carries one atomic specialty concept — no specialty grouping occurs here

This design means a downstream measurable can filter `DxRelevantVisitMV` by
`provider_specialty_concept_id` and treat the result as an event stream, with all
grouping, de-duplication, and timing composition deferred to the measure engine.

## Treatment Window And Consult Window Pattern

The episode layer currently exposes two important scalar-style constructs:

- `TreatmentEnvelopeMV`
  earliest/latest treatment and treatment-derived scalar windows
- `ConsultWindowMV`
  referral-derived specialist and treatment windows (scalar convenience layer —
  see below)

`ConsultWindowMV` is built by combining:

- episode-of-care anchors
- diagnosis-linked consult observations from `DxObservationMV`
- episode-linked provider-specialty visits from `DxRelevantVisitMV`
- earliest treatment from `TreatmentEnvelopeMV`

### ConsultWindowMV vs DxRelevantVisitMV

| | `DxRelevantVisitMV` | `ConsultWindowMV` |
|---|---|---|
| Shape | one row per (visit, episode) | one scalar row per episode |
| Specialty | atomic concept per row | groups hardcoded specialty sets |
| Aggregation | none | `min(visit_start_date)` across specialty groups |
| Purpose | reusable event surface | oncology referral-timing scalars |
| Status | active, first-class | retained pending downstream migration |

`ConsultWindowMV` is a **scalar convenience layer** specific to oncology
referral-timing indicators. Its timing logic is superseded by the
temporal-window pattern in `oa_cohorts`. It must remain in place until
downstream measures complete migration; it should not be extended with new
specialty groups or timing variants.

## Two-Measurable Temporal-Window Pattern

The preferred pattern for referral-timing indicators is:

1. Define an observation measurable from `DxObservationMV` — e.g. GP oncology referral
   (filtered by `event_concept_id`) — as the **anchor event**.
2. Define one or more visit measurables from `DxRelevantVisitMV` — e.g. one per
   specialist specialty concept — as **candidate events**.
3. Pass all candidate measurables to `measure_temporal_window` in `oa_cohorts`.
   The window engine takes the minimum over all candidates and computes the
   elapsed days against the anchor.

Example — "GP referral to first oncology specialist" indicator:

```
# Anchor: GP oncology referral observation
referral_obs = ObservationMeasurable(
    construct=DxObservationMV,
    person_id_attr="person_id",
    episode_id_attr="episode_id",
    event_date_attr="event_date",
    value_concept_attr="event_concept_id",
    concept_filter=[oncology_referral_concept_id],
)

# Candidates: specialist visits by specialty
medonc_visit = VisitMeasurable(
    construct=DxRelevantVisitMV,
    person_id_attr="person_id",
    episode_id_attr="episode_id",
    event_date_attr="visit_start_date",
    value_concept_attr="provider_specialty_concept_id",
    concept_filter=[medonc_concept_id],
)

radonc_visit = VisitMeasurable(
    construct=DxRelevantVisitMV,
    ...
    concept_filter=[radonc_concept_id],
)

haematology_visit = VisitMeasurable(
    construct=DxRelevantVisitMV,
    ...
    concept_filter=[haematologist_concept_id],
)

# Window engine (in oa_cohorts) combines candidates and measures against anchor
measure_temporal_window(
    anchor=referral_obs,
    candidates=[medonc_visit, radonc_visit, haematology_visit],
    threshold_days=X,
)
```
