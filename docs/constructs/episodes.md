# Episode constructs

From `omop_constructs.alchemy.episodes`.

Episode constructs represent clinical episodes and the treatment activity within them. The central organizing entity throughout is the condition episode, to which treatment and event data is attributed.

Treatment recorded on a progression or metastatic diagnosis remains available
on that diagnosis and also counts for its valid same-person episode-of-care
parent. Attribution follows one recorded parent. Siblings and
other primaries do not inherit the evidence. Generic events use their own
attachment rules, and indicator windows determine report eligibility.

---

## `ConditionEpisodeMV`

All disease episodes as a materialized view. One row per episode. Covers episodes of care, disease progression episodes, and metastatic episodes. This is the root entity that all episode-attributed constructs join to.

---

## `OverarchingDiseaseEpisodeMV`

Episode-of-care rows optionally joined to their child extent episodes (disease progression or metastatic). Provides a two-level view of the disease hierarchy without requiring separate joins.

---

## `SurgicalProcedureMV`

Cancer-relevant surgical procedures attributed to condition episodes.

**Source:** `Procedure_Occurrence` records whose concept is a descendant of the broad surgical-procedure ancestor, excluding radiotherapy and radioisotope descendants. Surgical history observations (i.e. procedures reported as prior history rather than performed procedures) are excluded — their timestamps record when the history was noted, not when the surgery happened, so they are not suitable for treatment timing.

**Episode attribution:** Typed, same-person `Episode_Event` linkage supplies the
recorded diagnosis owner. Valid child evidence also counts for the root, with each
performed source procedure deduplicated per target, including one linked to both root
and child. There is no date-window fan-out across overlapping primaries.

`ConditionEpisodeMV` remains the spine, so **every** condition episode appears: one with no valid own or inherited performed surgery contributes exactly one row with all surgery columns NULL. That null spine is a public contract — oa-cohorts absence rules identify non-surgical episodes with `WHERE surgery_concept_id IS NULL`.

Radioisotope therapy shares the `Procedure_Occurrence` source but is a separate construct (`RadioisotopeMV`) and does still attach by date window. The asymmetry is deliberate.

**Key fields:** `person_id`, `condition_episode_id`, `condition_start_date`, `surgery_datetime`, `surgery_name`, `surgery_concept_id`, `surgery_concept_code`, `surgery_source`.

**oa-cohorts:** Accessed via `RuleTarget.tx_surgical`. The `event_date_attr` is `surgery_datetime` and the `episode_id_attr` is `condition_episode_id`, so rules are anchored to the date of surgery within the correct episode.

---

## `SACTRegimenMV`

Systemic anti-cancer therapy (SACT) detail, one row per attributed diagnosis ×
cycle × prescription × intent measurement. Prescription and intent may be NULL.
Each cycle carries its first/last exposure dates and distinct physical exposure
count. Recorded regimen ownership supplies the direct diagnosis and, for an
eligible nested diagnosis, its episode-of-care root. Multiple same-concept intents stay
separate through `intent_id`.

**Key fields:** `condition_episode_id`, `first_exposure_date`, `last_exposure_date`, `regimen_concept`, `intent_concept`.

**oa-cohorts:** Accessed via `RuleTarget.tx_chemotherapy` (through `ConditionTreatmentEpisode`).

---

## `RTCourseMV`

Radiotherapy detail, one row per attributed diagnosis × fraction episode ×
prescription × intent measurement. Prescription and intent may be NULL. Dates and
distinct performed-procedure counts come from the fraction before detail fan-out.
Recorded course ownership supplies the direct diagnosis and its eligible
episode-of-care root.
Current MOSAIQ courses use treatment-regimen episode type 32531; independently
typed radiotherapy courses (32940) remain separate compatibility work.

**Key fields:** `condition_episode_id`, `first_exposure_date`, `last_exposure_date`, `course_concept`, `intent_concept`.

**oa-cohorts:** Accessed via `RuleTarget.tx_radiotherapy` (through `ConditionTreatmentEpisode`).

---

## `CycleMV`

Individual treatment cycles within a SACT regimen. Provides drug-exposure level detail below the regimen. Summarised per cycle by `SACTRegimenMV`.

---

## `FractionMV`

Individual radiotherapy fractions within a course. Provides procedure × fraction × intent-measurement detail below the RT course, retaining nullable `intent_id`. Summarised per fraction episode by `RTCourseMV`.

---

## `TreatmentEnvelopeMV`

Treatment timing for each condition episode across all modalities (surgery, SACT, and RT), joined to death information. There is one row per condition start date in the episode, so an episode whose linked conditions start on different dates has several rows with the same treatment dates. The primary source for treatment timing indicators in oa-cohorts.

**Modality coverage:** All three treatment types contribute to both the earliest and latest treatment dates, each pre-aggregated to episode grain before being joined so no modality can multiply the envelope. Surgery uses `SurgicalProcedureMV`; SACT and RT use their respective episode-linked MVs.

`LEAST` and `GREATEST` here rely on PostgreSQL ignoring NULL inputs and returning NULL only when every input is NULL. This is intentional: a missing modality must not suppress a real date from another.

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

Note that all three window measurables use `condition_start_date` as their `event_date_attr`. The temporal anchor in oa-cohorts is the row's condition start date, not the treatment date itself; the numeric or predicate value carries the timing information.

---

## `TreatmentRegimenCycleMV`

Treatment regimen rows with optional linked cycle episodes. Provides a hierarchical view of regimen → cycle without joining to condition context.

---

## `ConditionTreatmentEpisode`

Treatment summary joining condition context to SACT and RT summaries, including
valid nested-diagnosis evidence. Its rows represent regimen/intent-concept × course/intent-concept
product. Physical source counts precede detail fan-out; regimen/course counts are
distinct IDs within each summary group (normally one), not totals for the diagnosis.
Do not count wide rows as treatments or patients.

**oa-cohorts:** Accessed via `RuleTarget.tx_chemotherapy` and `RuleTarget.tx_radiotherapy`.

---

## `DxTreatStartMV`

Diagnosis-to-treatment timing summary. One row per condition episode that has at least one valid own or inherited treatment episode. Exposes `treatment_start` (earliest regimen start) and `treatment_end` (latest regimen end) relative to the diagnosis episode.

**oa-cohorts:** Accessed via `RuleTarget.tx_current_episode`.

---

## `TreatmentIntentMV` / `ConditionTreatmentIntentMV`

Treatment intent measurements recorded directly against treatment episodes.
`TreatmentIntentMV` retains the recorded owner and exposes `person_id` and
`intent_measurement_id`. `ConditionTreatmentIntentMV` additionally credits valid
roots and carries `intent_measurement_id`; it preserves one null-intent row when
none exists. Prescription-procedure intent is separate detail in the SACT/RT views.
Concurrency is reduced to one person/episode value before this join, so multiple
condition start dates do not multiply intent. Modality flags still require events
linked directly to the measured treatment episode; activity on descendants is
not inferred.

**oa-cohorts:** Accessed via `RuleTarget.intent_sact` and `RuleTarget.intent_rt`.

---

## `ConsultWindowMV`

Episode-of-care consult and referral window scalars. Provides `referral_to_specialist` (days from initial GP referral to specialist) and `referral_to_tx` (days from referral to first treatment).

**oa-cohorts:** Accessed via `RuleTarget.referral_to_specialist_window`.

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
| [`ConditionEpisodeMV`](#contract-condition_episode_mv) | disease episode of any type |
| [`ConditionTreatmentEpisode`](#contract-condition_treatment_episode_mv) | condition occurrence × regimen/intent-concept × course/intent-concept combination |
| [`ConditionTreatmentIntentMV`](#contract-condition_episode_intent_mv) | Own diagnosis evidence and valid same-person child-to-root evidence |
| [`ConsultWindowMV`](#contract-consult_window_mv) | episode of care |
| [`CycleMV`](#contract-cycle_mv) | distinct (drug exposure |
| [`DxTreatStartMV`](#contract-dx_treat_start_mv) | disease target with treatment |
| [`FractionMV`](#contract-fraction_mv) | Procedure/intent detail remains explicit; duplicate links collapse before mv_id |
| [`OverarchingDiseaseEpisodeMV`](#contract-overarching_disease_episode_mv) | episode of care + child disease-extent episode |
| [`RadioisotopeMV`](#contract-radioisotope_mv) | condition episode + radioisotope procedure inside the episode date window |
| [`RTCourseMV`](#contract-rt_course_mv) | RT detail remains cycle/fraction detail |
| [`SACTRegimenMV`](#contract-sact_treatment_mv) | SACT detail remains cycle/fraction detail |
| [`SurgicalProcedureMV`](#contract-surgical_procedure_mv) | target diagnosis + performed procedure |
| [`TreatmentEnvelopeMV`](#contract-treatment_envelope_mv) | condition episode + condition start date |
| [`TreatmentIntentMV`](#contract-episode_treatment_mv) | treatment episode + treatment-intent measurement on that episode |
| [`TreatmentRegimenCycleMV`](#contract-treatment_regimen_cycle_mv) | treatment regimen episode + child treatment cycle episode |

### `ConditionEpisodeMV` {#contract-condition_episode_mv}

View: `condition_episode_mv`

**Rows:** One row per disease episode of any type: episode of care, disease progression, or metastatic. The root entity every episode-attributed construct joins to.

**Unique on:** `episode_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `ConditionTreatmentEpisode` {#contract-condition_treatment_episode_mv}

View: `condition_treatment_episode_mv`

**Rows:** One row per condition occurrence × regimen/intent-concept × course/intent-concept combination. The N×M compatibility product and existing condition date spine remain.

**Unique on:** `condition_episode_id`, `regimen_id`, `course_id`. `condition_episode_id`, `regimen_id`, `course_id` are NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `ConditionTreatmentIntentMV` {#contract-condition_episode_intent_mv}

View: `condition_episode_intent_mv`

**Rows:** Own diagnosis evidence and valid same-person child-to-root evidence, without sibling propagation. Envelope concurrency is reduced to one person/episode row before the join.

**Unique on:** episode_id, treatment_episode_id, intent_measurement_id (NULLS NOT DISTINCT).

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `ConsultWindowMV` {#contract-consult_window_mv}

View: `consult_window_mv`

**Rows:** One row per episode of care, carrying the earliest GP referral, earliest specialist contact, earliest palliative-care contact, and the derived referral-to-specialist and referral-to-treatment day counts. An episode whose linked conditions start on more than one date appears on several rows with the same values.

**Unique on:** No published column combination is guaranteed unique: `episode_id` can repeat on rows with the same values.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `CycleMV` {#contract-cycle_mv}

View: `cycle_mv`

**Rows:** One row per distinct (drug exposure, treatment cycle), after same-person cycle/regimen/disease-owner and episode-type guards.

**Unique on:** `drug_exposure_id`, `cycle_id`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `DxTreatStartMV` {#contract-dx_treat_start_mv}

View: `dx_treat_start_mv`

**Rows:** One row per disease target with treatment: own recorded treatment episodes plus treatment of valid same-person progression/metastatic children for an episode-of-care root. Dates are MIN/MAX and treatment_regimen_count counts distinct accepted treatment episode IDs.

**Unique on:** dx_episode_id; no-treatment diagnoses remain absent.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `FractionMV` {#contract-fraction_mv}

View: `fraction_mv`

**Rows:** Procedure/intent detail remains explicit; duplicate links collapse before mv_id. Count distinct physical procedure IDs in aggregates.

**Unique on:** procedure_occurrence_id, fraction_id, intent_id (NULLS NOT DISTINCT).

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `OverarchingDiseaseEpisodeMV` {#contract-overarching_disease_episode_mv}

View: `overarching_disease_episode_mv`

**Rows:** One row per (episode of care, child disease-extent episode) pair. An episode of care with no progression or metastatic child contributes one row with the extent columns NULL.

**Unique on:** `episode_id`, `extent_episode_id`. `extent_episode_id` is NULL on rows with nothing to link.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `RadioisotopeMV` {#contract-radioisotope_mv}

View: `radioisotope_mv`

**Rows:** One row per (condition episode, radioisotope procedure inside the episode date window). Every condition episode appears, with a null spine row when no radioisotope procedure falls in the window.

**Unique on:** `condition_episode_id`, `ri_occurrence_id`. `ri_occurrence_id` is NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `RTCourseMV` {#contract-rt_course_mv}

View: `rt_course_mv`

**Rows:** RT detail remains cycle/fraction detail, not a regimen/course stream. Physical counts are deduplicated before prescription/intent and hierarchy expansion.

**Unique on:** condition_episode_id, fraction_id, course_prescription_id, intent_id (NULLS NOT DISTINCT).

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `SACTRegimenMV` {#contract-sact_treatment_mv}

View: `sact_treatment_mv`

**Rows:** SACT detail remains cycle/fraction detail, not a regimen/course stream. Physical counts are deduplicated before prescription/intent and hierarchy expansion.

**Unique on:** condition_episode_id, cycle_id, regimen_prescription_id, intent_id (NULLS NOT DISTINCT).

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `SurgicalProcedureMV` {#contract-surgical_procedure_mv}

View: `surgical_procedure_mv`

**Rows:** One row per (target diagnosis, performed procedure), including valid same-person child-to-root credit. Each target without performed surgery contributes exactly one null surgery row.

**Unique on:** `condition_episode_id`, `surgery_occurrence_id`. `surgery_occurrence_id` is NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `TreatmentEnvelopeMV` {#contract-treatment_envelope_mv}

View: `treatment_envelope_mv`

**Rows:** One row per (condition episode, condition start date), carrying the episode's earliest and latest treatment across surgery, SACT, and RT, the concurrent-chemoradiotherapy flag, death, and the derived day-count scalars. An episode whose linked conditions start on different dates has one row per start date.

**Unique on:** `condition_episode`, `condition_start_date`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `TreatmentIntentMV` {#contract-episode_treatment_mv}

View: `episode_treatment_mv`

**Rows:** One row per (treatment episode, treatment-intent measurement on that episode), with RT and SACT modality evidence flags derived from events linked directly to the episode.

**Unique on:** treatment_episode_id, intent_measurement_id.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `TreatmentRegimenCycleMV` {#contract-treatment_regimen_cycle_mv}

View: `treatment_regimen_cycle_mv`

**Rows:** One row per (treatment regimen episode, child treatment cycle episode) pair. A regimen with no cycles contributes one row with the cycle columns NULL.

**Unique on:** `episode_id`, `cycle_episode_id`. `cycle_episode_id` is NULL on rows with nothing to link.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

<!-- END GENERATED: construct-contracts -->
