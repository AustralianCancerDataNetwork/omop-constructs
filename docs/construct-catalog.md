# Construct Catalog

This page describes the public construct surface exposed by `omop-constructs` and how each construct fits into the broader data pipeline. Constructs are materialized views registered in the construct lifecycle; query factories and fragments are supporting infrastructure.

---

## Episode Constructs

From `omop_constructs.alchemy.episodes`.

Episode constructs represent clinical episodes and the treatment activity within them. The central organizing entity throughout is the condition episode, to which all treatment and event data is ultimately attributed.

---

### `ConditionEpisodeMV`

All disease episodes as a materialized view. One row per episode. Covers episodes of care, disease progression episodes, and metastatic episodes. This is the root entity that all episode-attributed constructs join to.

---

### `OverarchingDiseaseEpisodeMV`

Episode-of-care rows optionally joined to their child extent episodes (disease progression or metastatic). Provides a two-level view of the disease hierarchy without requiring separate joins.

---

### `SurgicalProcedureMV`

Cancer-relevant surgical procedures attributed to condition episodes.

**Source:** `Procedure_Occurrence` records with explicit dates. Surgical history observations (i.e. procedures reported as prior history rather than performed procedures) are excluded — they are not suitable for treatment timing and are not episode-attributed here.

**Episode attribution:** Currently, surgery records do not carry an OMOP `Episode_Event` link, so attribution uses a date window relative to each condition episode:
- Look back up to 90 days before episode start (captures diagnostic or staging surgery performed just prior to formal diagnosis).
- Look forward to the episode end date when one is present. For open-ended episodes, allow up to 365 days after episode start.
- A surgical procedure that falls outside this window for every condition episode the patient has does not appear in this view.

**Key fields:** `person_id`, `condition_episode_id`, `condition_start_date`, `surgery_datetime`, `surgery_name`, `surgery_concept_id`, `surgery_concept_code`, `surgery_source`.

**oa-cohorts:** Accessed via `RuleTarget.tx_surgical`. The `event_date_attr` is `surgery_datetime` and the `episode_id_attr` is `condition_episode_id`, so rules are anchored to the date of surgery within the correct episode.

---

### `SACTRegimenMV`

Systemic anti-cancer therapy (SACT) regimen episodes. One row per regimen per condition episode. Regimen episodes are explicitly linked to their parent condition episode via `Episode.episode_parent_id`, making episode attribution exact rather than date-inferred.

**Key fields:** `condition_episode_id`, `first_exposure_date`, `last_exposure_date`, `regimen_concept`, `intent_concept`.

**oa-cohorts:** Accessed via `RuleTarget.tx_chemotherapy` (through `ConditionTreatmentEpisode`).

---

### `RTCourseMV`

Radiotherapy course episodes. One row per course per condition episode. Like SACT, RT courses are explicitly linked to their parent condition episode via `Episode.episode_parent_id`.

**Key fields:** `condition_episode_id`, `first_exposure_date`, `last_exposure_date`, `course_concept`, `intent_concept`.

**oa-cohorts:** Accessed via `RuleTarget.tx_radiotherapy` (through `ConditionTreatmentEpisode`).

---

### `CycleMV`

Individual treatment cycles within a SACT regimen. Provides drug-exposure level detail below the regimen. Aggregated up to regimen level by `SACTRegimenMV`.

---

### `FractionMV`

Individual radiotherapy fractions within a course. Provides procedure-level detail below the RT course. Aggregated up to course level by `RTCourseMV`.

---

### `TreatmentEnvelopeMV`

Episode-level treatment timing summary across all modalities (surgery, SACT, and RT), joined to death information. The primary source for treatment timing indicators in oa-cohorts.

**Modality coverage:** All three treatment types contribute to both the earliest and latest treatment dates. Surgery uses `SurgicalProcedureMV` (episode-attributed via date window); SACT and RT use their respective episode-linked MVs.

**Key fields:**

| Field | Type | Description |
|---|---|---|
| `earliest_treatment` | Date | First treatment event across all modalities for the episode |
| `latest_treatment` | Date | Last treatment event across all modalities for the episode |
| `days_from_dx_to_treatment` | Integer | Calendar days from `condition_start_date` to `earliest_treatment`. Null when either is absent. |
| `treatment_days_before_death` | Integer | Calendar days from `latest_treatment` to death. Null when either is absent. Negative values indicate a data quality issue (treatment recorded after death) and are surfaced intentionally for downstream handling. |
| `concurrent_chemort` | Boolean / Null | True when SACT and RT windows overlap within the episode. Null when either modality is absent. Same-day starts are treated as concurrent. |
| `death_datetime` | DateTime | From the OMOP Death table. |

**oa-cohorts:** Three `RuleTarget` entries draw from this view:

- `RuleTarget.dx_to_tx_window` — the `days_from_dx_to_treatment` scalar, anchored temporally to `condition_start_date`.
- `RuleTarget.tx_to_death_window` — the `treatment_days_before_death` scalar, anchored temporally to `condition_start_date`.
- `RuleTarget.tx_concurrent` — the `concurrent_chemort` predicate, anchored temporally to `condition_start_date`.

Note that all three window measurables use `condition_start_date` as their `event_date_attr`. The temporal anchor in oa-cohorts is the episode start, not the treatment date itself; the numeric or predicate value carries the timing information.

---

### `TreatmentRegimenCycleMV`

Treatment regimen rows with optional linked cycle episodes. Provides a hierarchical view of regimen → cycle without joining to condition context.

---

### `ConditionTreatmentEpisode`

Treatment summary view joining condition episode context to SACT and RT summaries. Provides one row per treatment episode (regimen or course) carrying the parent condition episode metadata alongside the treatment dates and concept.

**oa-cohorts:** Accessed via `RuleTarget.tx_chemotherapy` and `RuleTarget.tx_radiotherapy`.

---

### `DxTreatStartMV`

Diagnosis-to-treatment timing summary. One row per condition episode that has at least one linked treatment regimen. Exposes `treatment_start` (earliest regimen start) and `treatment_end` (latest regimen end) relative to the diagnosis episode.

**oa-cohorts:** Accessed via `RuleTarget.tx_current_episode`.

---

### `TreatmentIntentMV` / `ConditionTreatmentIntentMV`

Treatment intent events. `TreatmentIntentMV` exposes raw intent records; `ConditionTreatmentIntentMV` joins them back to condition episode context. Intents are sourced from the modifier layer on regimen and course prescription procedures.

**oa-cohorts:** Accessed via `RuleTarget.intent_sact` and `RuleTarget.intent_rt`.

---

### `ConsultWindowMV`

Episode-of-care consult and referral window scalars. Provides `referral_to_specialist` (days from initial GP referral to specialist) and `referral_to_tx` (days from referral to first treatment).

**oa-cohorts:** Accessed via `RuleTarget.referral_to_specialist_window`.

---

## Event Constructs

From `omop_constructs.alchemy.events`.

Event constructs attach individual clinical events to condition episodes. Unlike episode constructs (which follow OMOP episode hierarchy links), event constructs use a two-tier attachment strategy implemented in `event_factories.py`:

1. **Explicit link:** Join through `Episode_Event` where the event has a registered episode linkage.
2. **Time-window fallback:** Attach by date when no explicit link exists. The default window is 90 days prior to episode start through the episode end date (or 365 days after episode start for episodes with no end date). Events falling outside this window for every condition episode the patient has do not appear in the view.

This strategy means each event row in the output is attributed to one specific condition episode, not spread across all episodes for the patient.

---

### `DxProcedureMV`

All diagnosis-linked procedure occurrences. One row per procedure per condition episode it falls within. Carries `episode_delta_days` — the signed integer number of days between the procedure and the episode start date.

**oa-cohorts:** Accessed via `RuleTarget.proc_concept`.

---

### `DxMeasurementMV`

Generic diagnosis-linked measurement surface. Focused slices derived from this base include:

- `WeightDxMV`, `WeightChangeDxMV`, `HeightDxMV`, `BSADxMV`
- `CreatinineClearanceDxMV`, `EGFRDxMV`, `FEV1DxMV`
- `DistressThermometerDxMV`, `ECOGDxMV`, `SmokingPYHDxMV`

Each slice filters to a specific measurement concept set and is episode-attributed via the same two-tier attachment strategy.

**oa-cohorts:** Accessed via `RuleTarget.meas_concept`.

---

### `DxObservationMV`

Diagnosis-linked observations. Episode-attributed via the two-tier attachment strategy.

**oa-cohorts:** Accessed via `RuleTarget.obs_concept`.

---

### `DxRelevantVisitMV`

Episode-linked visit occurrences with resolved provider specialty. Each row is one visit occurrence assigned to one condition episode, carrying a single atomic specialty concept. Multiple visits per episode appear as separate rows; no specialty grouping or within-episode aggregation is performed here.

**oa-cohorts:** Accessed via `RuleTarget.ev_visit`.

---

## Modifier Constructs

From `omop_constructs.alchemy.modifiers`.

Modifier constructs attach clinical annotations (stage, grade, laterality, size, metastatic status) to condition occurrences and episodes.

- `TStageMV`, `NStageMV`, `MStageMV`, `GroupStageMV` — TNM and group stage modifiers
- `AllStageModifierMV` — combined stage modifier surface
- `GradeModifierMV`, `LateralityModifierMV`, `SizeModifierMV`, `MetastaticDiseaseModifierMV` — additional modifier views
- `StageModifier` — unified stage-oriented materialized view
- `ModifiedCondition` — condition occurrences joined to episode and modifier context; used as the spine of most episode-level constructs
- `ModifiedProcedure` — procedure-level modifier surface; used to resolve regimen and course prescriptions with intent context

---

## Demography Constructs

From `omop_constructs.alchemy.demography`.

- `PersonDemography` — demographic attributes (age, sex, etc.) attached to condition episodes

---

## Condition Constructs

From `omop_constructs.alchemy.conditions`.

- `Condition_Window` — mapped condition window query surface

---

## Supporting Infrastructure

The following modules are not construct registries but are part of the active public architecture:

- `omop_constructs.core` — registry, planning, DDL, and materialized view lifecycle helpers
- `omop_constructs.alchemy.events.event_factories` — generic event-to-episode attachment functions, including `attach_to_condition_episode_via_episode_event`, `attach_to_condition_episode_by_time_window`, and `episode_relevant_window`. Default window constants (`DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90`, `DEFAULT_EPISODE_WINDOW_DAYS_POST = 365`, `DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365`) are defined here and shared across all constructs that perform date-window attachment.
- `omop_constructs.alchemy.episodes.episode_factories` — reusable episode query builders including `get_episode_query`, `get_episode_hierarchy_query`, and `dx_treatment_window`
- `omop_constructs.semantics` — runtime concept resolvers

---

## Catalog Scope

This catalog covers construct classes currently exported by the package. The rule of thumb for inclusion:

- If it is a mapped class with `__mv_name__`, it belongs in the construct lifecycle and appears here.
- If it is a query factory or query fragment, it is supporting infrastructure and appears in the supporting infrastructure section only.
