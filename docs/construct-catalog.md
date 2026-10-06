# Construct Catalog

This page describes the public construct surface exposed by `omop-constructs` and how each construct fits into the broader data pipeline. Constructs are materialized views registered in the construct lifecycle; query factories and fragments are supporting infrastructure.

Each construct family has its own page, listed under [Constructs by family](#constructs-by-family).

---

## Condition Constructs

From `omop_constructs.alchemy.conditions`.

- `Condition_Window` — mapped condition window query surface

---

## Supporting Infrastructure

The following modules are not construct registries but are part of the active public architecture:

- `omop_constructs.core` — registry, planning, DDL, and materialized view lifecycle helpers
- `omop_constructs.alchemy.events.event_factories` — query factories for canonical clinical-event projection, explicit episode relationships, and ranked date-based attachment. Callers can select an `EpisodeAttachmentPolicy`, provide a `TemporalRankingSpec`, and configure the default window constants (`DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90`, `DEFAULT_EPISODE_WINDOW_DAYS_POST = 365`, and `DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365`).
- `omop_constructs.alchemy.episodes.episode_factories` — reusable episode query builders including `get_episode_query`, `get_episode_hierarchy_query`, and `dx_treatment_window`
- `omop_constructs.semantics` — runtime concept resolvers

---

## Catalog Scope

This catalog covers construct classes currently exported by the package. The rule of thumb for inclusion:

- If it is a mapped class with `__mv_name__`, it belongs in the construct lifecycle and appears in this catalogue.
- If it is a query factory or query fragment, it is supporting infrastructure and appears in the supporting infrastructure section only.

---

<!-- BEGIN GENERATED: construct-contracts -->

<!-- Generated from construct-contracts.toml. Do not edit by hand; run `python -m omop_constructs.core.catalogue docs/construct-catalog.md`. -->

## Constructs by family

The package registers 43 constructs. Each family page describes its constructs and lists what one row represents, how rows are identified, and whether row IDs can be stored.

| Family | Constructs |
|---|---|
| [Episode constructs](constructs/episodes.md) | 15 |
| [Event constructs](constructs/events.md) | 14 |
| [Modifier constructs](constructs/modifiers.md) | 13 |
| [Demography constructs](constructs/demography.md) | 1 |

<!-- END GENERATED: construct-contracts -->
