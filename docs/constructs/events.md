# Event constructs

From `omop_constructs.alchemy.events`.

Event constructs attach individual clinical events to condition episodes. Unlike episode constructs (which follow OMOP episode hierarchy links), event constructs use a two-tier attachment strategy implemented in `event_factories.py`:

1. **Explicit link:** Accept an `Episode_Event` relationship only when its event ID, OMOP Field-concept discriminator, episode ID, and person all agree.
2. **Time-window fallback:** Attach by date only when the event has no valid explicit link. The default window is 90 days prior to episode start through the episode end date (or 365 days after episode start for episodes with no end date). Only episodes of care are candidates, and events falling outside this window for every episode of care the patient has do not appear in the view. Progression and metastatic episodes are never chosen by date; they receive events only through valid explicit links.

The construct family uses the `explicit_first_ranked` policy. A valid explicit row suppresses date-based fallback for that table-scoped event, while multiple valid explicit links remain distinct. An unlinked event attaches to one eligible episode of care, preferring an episode already started at the event date, then the nearest start, then the lowest episode ID. Exact `(source table, event ID, episode ID)` duplicates are removed. For example, an unlinked spirometry result that falls within two lung cancer episode windows is assigned to one episode by these rules; a result explicitly linked to both episodes remains associated with both. No construct in this family has a database unique key; each construct's key is listed under [Contracts](#contracts).

---

## `DxProcedureMV`

All diagnosis-linked procedure occurrences. Fallback attribution produces one episode per procedure; multiple valid explicit links remain distinct. Carries `episode_delta_days` — the signed integer number of days between the procedure and the episode start date.

**oa-cohorts:** Accessed via `RuleTarget.proc_concept`.

---

## `DxMeasurementMV`

Generic diagnosis-linked measurement surface. Focused slices derived from this base include:

- `WeightDxMV`, `WeightChangeDxMV`, `HeightDxMV`, `BSADxMV`
- `CreatinineClearanceDxMV`, `EGFRDxMV`, `FEV1DxMV`
- `DistressThermometerDxMV`, `ECOGDxMV`, `SmokingPYHDxMV`

Each slice filters to a specific measurement concept set and is episode-attributed via the same two-tier attachment strategy.

**oa-cohorts:** Accessed via `RuleTarget.meas_concept`.

---

## `DxObservationMV`

Diagnosis-linked observations. Episode-attributed via the two-tier attachment strategy.

**oa-cohorts:** Accessed via `RuleTarget.obs_concept`.

---

## `DxRelevantVisitMV`

Episode-linked visit occurrences with resolved provider specialty. Each row is one visit occurrence assigned to one condition episode, carrying a single atomic specialty concept. Multiple visits per episode appear as separate rows; no specialty grouping or within-episode aggregation is performed here.

**oa-cohorts:** Accessed via `RuleTarget.ev_visit`.

---

<!-- BEGIN GENERATED: construct-contracts -->

<!-- Generated from construct-contracts.toml. Do not edit by hand; run `python -m omop_constructs.core.catalogue docs/construct-catalog.md`. -->

## Contracts

What one row of each construct represents and how rows are identified.

- **Rows**: what one row represents.
- **Unique on**: the columns that together identify one row.
- **Row IDs**: whether the construct's own ID column can be stored and reused.

| Construct | One row per |
|---|---|
| [`BSADxMV`](#contract-bsa_dx_mv) | body surface area measurement + attributed condition episode |
| [`CreatinineClearanceDxMV`](#contract-creatinine_clearance_dx_mv) | creatinine clearance measurement + attributed condition episode |
| [`DistressThermometerDxMV`](#contract-dtherm_dx_mv) | distress thermometer score measurement + attributed condition episode |
| [`DxMeasurementMV`](#contract-dx_measurement_mv) | measurement + attributed condition episode |
| [`DxObservationMV`](#contract-dx_observation_mv) | observation + attributed condition episode |
| [`DxProcedureMV`](#contract-dx_procedure_mv) | procedure occurrence + attributed condition episode |
| [`DxRelevantVisitMV`](#contract-dx_visit_mv) | visit occurrence + attributed episode of care |
| [`ECOGDxMV`](#contract-ecog_dx_mv) | ECOG performance status measurement + attributed condition episode |
| [`EGFRDxMV`](#contract-egfr_dx_mv) | estimated glomerular filtration rate measurement + attributed condition episode |
| [`FEV1DxMV`](#contract-fev1_dx_mv) | FEV1 measurement + attributed condition episode |
| [`HeightDxMV`](#contract-height_dx_mv) | body height measurement + attributed condition episode |
| [`SmokingPYHDxMV`](#contract-smoking_pyh_dx_mv) | smoking pack-year history measurement + attributed condition episode |
| [`WeightChangeDxMV`](#contract-weight_change_dx_mv) | body weight change measurement + attributed condition episode |
| [`WeightDxMV`](#contract-weight_dx_mv) | body weight measurement + attributed condition episode |

### `BSADxMV` {#contract-bsa_dx_mv}

View: `bsa_dx_mv`

**Rows:** One row per (body surface area measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `CreatinineClearanceDxMV` {#contract-creatinine_clearance_dx_mv}

View: `creatinine_clearance_dx_mv`

**Rows:** One row per (creatinine clearance measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DistressThermometerDxMV` {#contract-dtherm_dx_mv}

View: `dtherm_dx_mv`

**Rows:** One row per (distress thermometer score measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DxMeasurementMV` {#contract-dx_measurement_mv}

View: `dx_measurement_mv`

**Rows:** One row per (measurement, attributed condition episode). The generic diagnosis-linked measurement surface, unrestricted by concept.

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DxObservationMV` {#contract-dx_observation_mv}

View: `dx_observation_mv`

**Rows:** One row per (observation, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DxProcedureMV` {#contract-dx_procedure_mv}

View: `dx_procedure_mv`

**Rows:** One row per (procedure occurrence, attributed condition episode), carrying the signed episode_delta_days.

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DxRelevantVisitMV` {#contract-dx_visit_mv}

View: `dx_visit_mv`

**Rows:** One row per (visit occurrence, attributed episode of care), carrying one atomic provider specialty. No within-episode aggregation and no specialty grouping.

**Unique on:** `visit_occurrence_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `ECOGDxMV` {#contract-ecog_dx_mv}

View: `ecog_dx_mv`

**Rows:** One row per (ECOG performance status measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `EGFRDxMV` {#contract-egfr_dx_mv}

View: `egfr_dx_mv`

**Rows:** One row per (estimated glomerular filtration rate measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `FEV1DxMV` {#contract-fev1_dx_mv}

View: `fev1_dx_mv`

**Rows:** One row per (FEV1 measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `HeightDxMV` {#contract-height_dx_mv}

View: `height_dx_mv`

**Rows:** One row per (body height measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `SmokingPYHDxMV` {#contract-smoking_pyh_dx_mv}

View: `smoking_pyh_dx_mv`

**Rows:** One row per (smoking pack-year history measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `WeightChangeDxMV` {#contract-weight_change_dx_mv}

View: `weight_change_dx_mv`

**Rows:** One row per (body weight change measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `WeightDxMV` {#contract-weight_dx_mv}

View: `weight_dx_mv`

**Rows:** One row per (body weight measurement, attributed condition episode).

**Unique on:** `event_id`, `episode_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

<!-- END GENERATED: construct-contracts -->
