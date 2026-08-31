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

**Source:** `Procedure_Occurrence` records whose concept is a descendant of the broad surgical-procedure ancestor, excluding radiotherapy and radioisotope descendants. Surgical history observations (i.e. procedures reported as prior history rather than performed procedures) are excluded — their timestamps record when the history was noted, not when the surgery happened, so they are not suitable for treatment timing.

**Episode attribution:** Explicit `Episode_Event` linkage. Pipeline-runtime chooses which diagnosis episode owns each surgery, so there is no date-window fan-out across overlapping primaries.

`ConditionEpisodeMV` remains the spine, so **every** condition episode appears: one with no linked surgery contributes exactly one row with all surgery columns NULL. That null spine is a public contract — oa-cohorts absence rules identify non-surgical episodes with `WHERE surgery_concept_id IS NULL`.

Radioisotope therapy shares the `Procedure_Occurrence` source but is a separate construct (`RadioisotopeMV`) and does still attach by date window. The asymmetry is deliberate.

**Key fields:** `person_id`, `condition_episode_id`, `condition_start_date`, `surgery_datetime`, `surgery_name`, `surgery_concept_id`, `surgery_concept_code`, `surgery_source`.

**oa-cohorts:** Accessed via `RuleTarget.tx_surgical`. The `event_date_attr` is `surgery_datetime` and the `episode_id_attr` is `condition_episode_id`, so rules are anchored to the date of surgery within the correct episode.

---

### `SACTRegimenMV`

Systemic anti-cancer therapy (SACT) regimen episodes, with first and last exposure dates aggregated from their cycles. Regimen episodes are explicitly linked to their parent condition episode via `Episode.episode_parent_id`, making episode attribution exact rather than date-inferred.

Its declared grain is one row per regimen episode. The current query is driven by cycle rows and multiplied by regimen prescription procedures, so it does not yet meet that grain — see `sact_treatment_mv` in the contract table below.

**Key fields:** `condition_episode_id`, `first_exposure_date`, `last_exposure_date`, `regimen_concept`, `intent_concept`.

**oa-cohorts:** Accessed via `RuleTarget.tx_chemotherapy` (through `ConditionTreatmentEpisode`).

---

### `RTCourseMV`

Radiotherapy course episodes, with first and last exposure dates aggregated from their fractions. Like SACT, RT courses are explicitly linked to their parent condition episode via `Episode.episode_parent_id`.

Its declared grain is one row per course episode. The current query is driven by fraction rows, and resolves the course episode with the treatment-regimen episode concept, so course and condition-episode metadata may be absent — see `rt_course_mv` in the contract table below.

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

**Modality coverage:** All three treatment types contribute to both the earliest and latest treatment dates, each pre-aggregated to episode grain before being joined so no modality can multiply the envelope. Surgery uses `SurgicalProcedureMV`; SACT and RT use their respective episode-linked MVs.

`LEAST` and `GREATEST` here rely on PostgreSQL ignoring NULL inputs and returning NULL only when every input is NULL. This is intentional: a missing modality must not suppress a real date from another.

Its declared grain is one row per condition episode. The current query is rooted in condition-occurrence rows with `condition_start_date` in its `DISTINCT`, so an episode with several condition records on different dates yields several rows — see `treatment_envelope_mv` in the contract table below.

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

Treatment summary view joining condition episode context to SACT and RT summaries, carrying the parent condition episode metadata alongside the treatment dates and concepts.

The regimen and course summaries are joined side by side, so an episode with N regimens and M courses produces N×M rows rather than N+M. `regimen_count` and `course_count` are each grouped by the entity they count and are therefore structurally always 1. See `condition_treatment_episode_mv` in the contract table below.

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

1. **Explicit link:** Accept an `Episode_Event` relationship only when its event ID, OMOP Field-concept discriminator, episode ID, and person all agree.
2. **Time-window fallback:** Attach by date only when the event has no valid explicit link. The default window is 90 days prior to episode start through the episode end date (or 365 days after episode start for episodes with no end date). Events falling outside this window for every condition episode the patient has do not appear in the view.

The construct family names `explicit_first_all_in_window` as its policy. A valid explicit row therefore occurs once and suppresses every fallback candidate for that table-scoped event. An unlinked event deliberately attaches to every overlapping eligible episode; exact `(source table, event ID, episode ID)` duplicates are removed. The factory paths remain compatibility wrappers over `omop_alchemy.toolkit.episodes.derivation`. No construct in this family currently has a database unique key; the declared keys and the remaining identity work are in the contract table below.

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

- `PersonDemography` — demographic attributes (gender, year of birth, death, MRN, postcode, country of birth, language spoken) attached to condition episodes. Declared grain is one row per person per condition episode; the postcode, country-of-birth, and language joins are currently unranked, so repeated observations multiply. See `person_demography_mv` in the contract table below.

---

## Condition Constructs

From `omop_constructs.alchemy.conditions`.

- `Condition_Window` — mapped condition window query surface

---

## Supporting Infrastructure

The following modules are not construct registries but are part of the active public architecture:

- `omop_constructs.core` — registry, planning, DDL, and materialized view lifecycle helpers
- `omop_constructs.alchemy.events.event_factories` — compatibility paths over OMOP Alchemy's canonical event projection and explicit-first attachment builder. New calls pass `EpisodeAttachmentPolicy` explicitly. The old Boolean and the standalone explicit/window helpers emit `DeprecationWarning` for one pre-1.0 compatibility cycle. Default window constants (`DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90`, `DEFAULT_EPISODE_WINDOW_DAYS_POST = 365`, `DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365`) remain available.
- `omop_constructs.alchemy.episodes.episode_factories` — reusable episode query builders including `get_episode_query`, `get_episode_hierarchy_query`, and `dx_treatment_window`
- `omop_constructs.semantics` — runtime concept resolvers

---

## Catalog Scope

This catalog covers construct classes currently exported by the package. The rule of thumb for inclusion:

- If it is a mapped class with `__mv_name__`, it belongs in the construct lifecycle and appears here.
- If it is a query factory or query fragment, it is supporting infrastructure and appears in the supporting infrastructure section only.

---

<!-- BEGIN GENERATED: construct-contracts -->

<!-- Generated from construct-contracts.toml. Do not edit by hand;
     run `python -m omop_constructs.core.catalogue docs/construct-catalog.md`. -->

## Construct contracts

Generated from [`construct-contracts.toml`](https://github.com/AustralianCancerDataNetwork/omop-constructs/blob/main/construct-contracts.toml),
the machine-readable grain catalogue. The manifest is the source of truth;
this section is a rendering of it.

### Declared grains and logical keys

Every registered construct, by family. **Key** is the *intended* logical
key — the one the construct must satisfy to enter a 1.0 release candidate.
Where a construct does not satisfy it today, **Findings** names why and the
register below explains each entry.

**Surrogate** distinguishes an identifier carried through from the CDM,
which is stable across refreshes and safe to reference, from a refresh-local
`row_number()`, which is unique within one materialization only and must not
be stored downstream.

#### Episodes

| Construct | Grain | Key | Surrogate | 1.0 | Lung | Findings |
|---|---|---|---|---|---|---|
| `condition_episode_intent_mv` | Declared: one row per (condition episode, child treatment episode with a recorded intent), with modality and concurrency flags. Condition episodes with no intent-bearing treatment episode contribute one row with the treatment columns NULL. | `episode_id`, `treatment_episode_id`, `treatment_intent_concept_id`<br>nullable: `treatment_episode_id`, `treatment_intent_concept_id`<br>**incomplete** | refresh-local | yes | direct | OC-H1, OC-H4, OC-M3, OC-0-N4 |
| `condition_episode_mv` | One row per disease episode of any type: episode of care, disease progression, or metastatic. The root entity every episode-attributed construct joins to. | `episode_id` | source id | yes | dep | — |
| `condition_treatment_episode_mv` | Declared: one row per (condition episode, regimen, course) treatment episode, carrying condition context alongside the regimen and course dates and concepts. | `condition_episode_id`, `regimen_id`, `course_id`<br>nullable: `condition_episode_id`, `regimen_id`, `course_id` | refresh-local | yes | direct | OC-H1, OC-M3, OC-0-N6, OC-0-N7 |
| `consult_window_mv` | Declared: one row per episode of care, carrying the earliest GP referral, earliest specialist contact, earliest palliative-care contact, and the derived referral-to-specialist and referral-to-treatment day counts. | `episode_id` | refresh-local | yes | — | OC-H1, OC-M3 |
| `cycle_mv` | One row per (drug exposure, treatment cycle episode). Drug-exposure level detail beneath the regimen. | `drug_exposure_id`, `cycle_id` | refresh-local | yes | dep | OC-M3, OC-0-N2 |
| `dx_treat_start_mv` | One row per disease episode that has at least one child treatment episode, carrying the earliest treatment start, latest treatment end, and count of distinct child regimens. | `dx_episode_id` | source id | yes | direct | — |
| `episode_treatment_mv` | One row per (treatment episode, treatment-intent measurement on that episode), with RT and SACT modality evidence flags derived from events linked directly to the episode. | `treatment_episode_id`, `treatment_intent_concept_id`<br>**incomplete** | refresh-local | yes | dep | OC-H4, OC-M3, OC-0-N4 |
| `fraction_mv` | Declared: one row per (radiotherapy procedure occurrence, fraction episode). Procedure-level detail beneath the RT course. | `procedure_occurrence_id`, `fraction_id` | refresh-local | yes | dep | OC-H1, OC-M3 |
| `overarching_disease_episode_mv` | One row per (episode of care, child disease-extent episode) pair. An episode of care with no progression or metastatic child contributes one row with the extent columns NULL. | `episode_id`, `extent_episode_id`<br>nullable: `extent_episode_id` | source id | yes | dep | OC-B3 |
| `radioisotope_mv` | One row per (condition episode, radioisotope procedure inside the episode date window). Every condition episode appears, with a null spine row when no radioisotope procedure falls in the window. | `condition_episode_id`, `ri_occurrence_id`<br>nullable: `ri_occurrence_id` | refresh-local | — | — | OC-M1, OC-M3 |
| `rt_course_mv` | Declared: one row per radiotherapy course episode, with first and last exposure dates aggregated from its fractions. | `course_id` | refresh-local | yes | dep | OC-H1, OC-M3, OC-0-N5, OC-0-N8 |
| `sact_treatment_mv` | Declared: one row per SACT regimen episode, with first and last exposure dates aggregated from its cycles. | `regimen_id` | refresh-local | yes | dep | OC-H1, OC-M3, OC-0-N8 |
| `surgical_procedure_mv` | One row per (condition episode, explicitly linked surgical procedure). Every condition episode appears: an episode with no linked surgery contributes exactly one row with all surgery columns NULL. That null spine is a public contract — oa-cohorts absence rules depend on it. | `condition_episode_id`, `surgery_occurrence_id`<br>nullable: `surgery_occurrence_id` | refresh-local | yes | direct | OC-M1, OC-M3 |
| `treatment_envelope_mv` | Declared: one row per condition episode, carrying earliest and latest treatment across surgery, SACT, and RT, the concurrent-chemoradiotherapy flag, death, and the derived day-count scalars. | `condition_episode` | refresh-local | yes | direct | OC-H1, OC-M3 |
| `treatment_regimen_cycle_mv` | One row per (treatment regimen episode, child treatment cycle episode) pair. A regimen with no cycles contributes one row with the cycle columns NULL. | `episode_id`, `cycle_episode_id`<br>nullable: `cycle_episode_id` | source id | yes | dep | OC-B3 |

#### Events

| Construct | Grain | Key | Surrogate | 1.0 | Lung | Findings |
|---|---|---|---|---|---|---|
| `bsa_dx_mv` | Declared: one row per (body surface area measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `creatinine_clearance_dx_mv` | Declared: one row per (creatinine clearance measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `dtherm_dx_mv` | Declared: one row per (distress thermometer score measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `dx_measurement_mv` | Declared: one row per (measurement, attributed condition episode). The generic diagnosis-linked measurement surface, unrestricted by concept. | `event_id`, `episode_id` | refresh-local | yes | direct | OC-M3 |
| `dx_observation_mv` | Declared: one row per (observation, attributed condition episode). | `event_id`, `episode_id` | refresh-local | yes | direct | OC-M3 |
| `dx_procedure_mv` | Declared: one row per (procedure occurrence, attributed condition episode), carrying the signed episode_delta_days. | `event_id`, `episode_id` | refresh-local | yes | direct | OC-M3 |
| `dx_visit_mv` | One row per (visit occurrence, attributed episode of care), carrying one atomic provider specialty. No within-episode aggregation and no specialty grouping. | `visit_occurrence_id`, `episode_id` | refresh-local | yes | — | OC-H3, OC-M3 |
| `ecog_dx_mv` | Declared: one row per (ECOG performance status measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `egfr_dx_mv` | Declared: one row per (estimated glomerular filtration rate measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `fev1_dx_mv` | Declared: one row per (FEV1 measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `height_dx_mv` | Declared: one row per (body height measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `smoking_pyh_dx_mv` | Declared: one row per (smoking pack-year history measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `weight_change_dx_mv` | Declared: one row per (body weight change measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |
| `weight_dx_mv` | Declared: one row per (body weight measurement, attributed condition episode). | `event_id`, `episode_id` | refresh-local | — | — | OC-M3 |

#### Modifiers

| Construct | Grain | Key | Surrogate | 1.0 | Lung | Findings |
|---|---|---|---|---|---|---|
| `all_stage_modifier_mv` | One row per stage measurement of any TNM or group-stage type. Deliberately unranked: this is the long-form stage stream, not a preferred-stage resolver. | `measurement_id` | source id | yes | dep | — |
| `grade_modifier_mv` | One row per modified event: the earliest recorded tumour grade measurement for that event. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `group_stage_mv` | One row per modified event: the preferred group stage for that event, earliest pathological if present, otherwise earliest clinical. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `laterality_modifier_mv` | One row per modified event: the earliest recorded tumour laterality measurement for that event. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `m_stage_mv` | One row per modified event: the preferred M stage for that event, earliest pathological if present, otherwise earliest clinical. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `metastatic_disease_modifier_mv` | One row per modified event: the earliest recorded metastatic-disease measurement for that event. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `modified_conditions_mv` | One row per (condition occurrence, linked condition episode), carrying the resolved T/N/M/group stage, grade, size, laterality, and metastatic-disease modifiers as columns. The spine of most episode-level constructs. | `condition_occurrence_id`, `condition_episode`<br>nullable: `condition_episode` | refresh-local | yes | dep | OC-M3 |
| `modified_procedure_mv` | One row per (procedure occurrence, treatment-intent modifier measurement). A procedure with no intent modifier contributes one row with the intent columns NULL. | `procedure_occurrence_id`, `intent_id`<br>nullable: `intent_id` | refresh-local | yes | dep | OC-M3 |
| `n_stage_mv` | One row per modified event: the preferred N stage for that event, earliest pathological if present, otherwise earliest clinical. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `primary_diagnosis_condition_mv` | One row per (condition occurrence, episode-of-care episode). modified_conditions_mv restricted to conditions linked to a top-level episode of care, with the episode start and end dates attached. | `condition_occurrence_id`, `condition_episode` | refresh-local | yes | direct | OC-M3 |
| `size_modifier_mv` | One row per modified event: the earliest recorded tumour size measurement for that event. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |
| `stage_modifier_mv` | One row per (condition occurrence, linked condition episode, stage measurement). The long-form stage stream with condition and episode context attached. | `condition_occurrence_id`, `condition_episode`, `stage_id`<br>nullable: `condition_episode` | refresh-local | yes | direct | OC-B3, OC-M3, OC-0-N3 |
| `t_stage_mv` | One row per modified event: the preferred T stage for that event, earliest pathological if present, otherwise earliest clinical. | `meas_event_field_concept_id`, `measurement_event_id`<br>nullable: `meas_event_field_concept_id`, `measurement_event_id` | source id | yes | dep | OC-0-N1 |

#### Demography

| Construct | Grain | Key | Surrogate | 1.0 | Lung | Findings |
|---|---|---|---|---|---|---|
| `person_demography_mv` | Declared: one row per (person, condition episode), carrying gender, year of birth, death, MRN, postcode, country of birth, and language spoken. | `person_id`, `episode_id` | refresh-local | yes | direct | OC-B4, OC-M3 |

### 1.0 public surface

32 of 43 registered constructs are part of the
public 1.0 surface: everything oa-cohorts imports, plus everything those
constructs depend on. The rest stay registered and buildable but are not
covered by the 1.0 contract, so their grain, keys, and column names may
change without a compatibility cycle.

**Not part of the 1.0 public surface:**

- `radioisotope_mv` — No current consumer. Not reachable from oa-cohorts' measurable registry or from any report in the shipped bundle. Kept registered and buildable, excluded from the 1.0 public surface.
- `bsa_dx_mv`, `creatinine_clearance_dx_mv`, `dtherm_dx_mv`, `ecog_dx_mv`, `egfr_dx_mv`, `fev1_dx_mv`, `height_dx_mv`, `smoking_pyh_dx_mv`, `weight_change_dx_mv`, `weight_dx_mv` — No current consumer; oa-cohorts resolves meas_concept to dx_measurement_mv. Excluded from the 1.0 public surface.

### Pre-production lung report

The lung report (`REP-000001`) resolves to 11 constructs directly and
reaches 30 in total once dependencies are included. Those are the
constructs whose results a release must explain a before/after delta for.

**Resolved directly by a lung report measure:**

- `condition_episode_intent_mv` — `intent_sact`, `intent_rt`, `intent_concurrent_rt`
- `condition_treatment_episode_mv` — `tx_chemotherapy`, `tx_radiotherapy`
- `dx_measurement_mv` — `meas_concept`, `meas_value_concept`
- `dx_observation_mv` — `obs_concept`
- `dx_procedure_mv` — `proc_concept`
- `dx_treat_start_mv` — `tx_current_episode`
- `person_demography_mv` — `demog_death`
- `primary_diagnosis_condition_mv` — `dx_primary`
- `stage_modifier_mv` — `dx_stage`
- `surgical_procedure_mv` — `tx_surgical`
- `treatment_envelope_mv` — `dx_to_tx_window`, `tx_to_death_window`, `tx_concurrent`

### Row-count behaviour of input joins

Only joins that can change the row count are listed. A join that
multiplies is where unexpected fan-out comes from; a join that drops rows
is where unexpected *absence* comes from, which is harder to notice.

**`person_demography_mv`**

- multiplies — condition_episode_mv: inner join on person_id; one row per episode, which is the intended grain
- multiplies — observation (postcode): outer join, unranked; every postcode observation the person has multiplies
- multiplies — observation (country of birth): outer join, unranked; multiplies independently
- multiplies — observation (language spoken): outer join, unranked; multiplies independently
- reduces — concept (gender): INNER join on gender_concept_id, so a person with a missing or zero gender concept is removed entirely
- _The three demographic observation joins are independent and unranked, so a person with N postcodes, M countries of birth, and K languages contributes N*M*K rows per episode._

**`condition_episode_intent_mv`**

- multiplies — episode_treatment_mv: outer join on treatment_episode_parent_id; several intent-bearing treatment episodes multiply, which is the intended grain
- multiplies — treatment_envelope_mv: outer join on condition_episode; multiplies again wherever the envelope has more than one row for the episode
- _Two multipliers stack here. The intended one is the treatment-episode join; the unintended one is the envelope join, which inherits OC-H1._

**`condition_episode_mv`**

- reduces — episode: restricted to the three disease episode types
- _get_episode_query() guarantees at most one row per episode_id: the only join is an outer lookup on the concept primary key._

**`condition_treatment_episode_mv`**

- multiplies — episode_event: outer join on the condition_occurrence_id discriminator
- multiplies — sact_treatment_mv: outer join on condition_episode_id after grouping by regimen and intent; several regimens multiply
- multiplies — rt_course_mv: outer join on condition_episode_id after grouping by course and intent; several courses multiply, producing a regimen-by-course product
- _Two independent one-to-many joins side by side: N regimens and M courses in one episode give N*M rows, not N+M. This is the clearest instance of OC-H1._

**`consult_window_mv`**

- multiplies — treatment_envelope_mv: outer join on condition_episode; the only multiplier in this query
- reduces — episode: spine, restricted to episode_of_care
- reduces — dx_observation_mv (specialist consult): min/max event_date grouped to episode grain
- reduces — dx_observation_mv (GP referral): min event_date grouped to episode grain
- reduces — dx_observation_mv (palliative referral): min event_date grouped to episode grain
- reduces — dx_visit_mv (specialist visit): min visit_start_date grouped to episode grain
- reduces — dx_visit_mv (palliative visit): min visit_start_date grouped to episode grain
- _All five consult and visit inputs are pre-aggregated to episode grain, so they are safe. The envelope join is not._

**`cycle_mv`**

- multiplies — episode_event: inner join on the drug_exposure_id discriminator; an exposure linked to several episodes multiplies
- reduces — drug_concept: drops exposures whose drug concept is absent
- reduces — route_concept: see OC-0-N2: drops exposures with no resolvable route
- reduces — cycle_concept: drops rows whose episode concept is absent

**`dx_treat_start_mv`**

- reduces — episode (diagnosis): restricted to the disease episode types
- reduces — episode (regimen): inner join on episode_parent_id, restricted to the treatment episode types, then GROUP BY the diagnosis episode
- _The inner join means episodes with no linked treatment are absent entirely, not present with NULL dates. The GROUP BY keys are all functionally determined by dx_episode_id, so the output is one row per episode. Note that __deps__ names condition_episode_mv and treatment_regimen_cycle_mv, but the SQL reads the Episode table directly; the declared dependencies only order the build._

**`episode_treatment_mv`**

- multiplies — measurement (intent): inner join on the episode_id modifier field, restricted to treatment_intent concepts; several intent measurements multiply
- reduces — intent_concept: drops rows whose intent concept is absent

**`fraction_mv`**

- multiplies — modified_procedure_mv: spine at procedure/intent grain, so a procedure with several intents multiplies
- multiplies — episode_event: inner join on the procedure_occurrence_id discriminator
- reduces — fraction_concept: joined on episode_object_concept_id; drops rows where it is absent
- reduces — rt_procedures concept set: restricted to resolved RT procedure concepts

**`overarching_disease_episode_mv`**

- multiplies — episode (child): outer join on episode_parent_id, restricted to disease_progression and metastatic; several children multiply the parent
- reduces — episode (parent): restricted to episode_of_care

**`radioisotope_mv`**

- multiplies — procedure_occurrence: outer join by person plus date window: 90 days before episode start through episode end, or 365 days after start when open-ended
- reduces — radioisotopes_only concept set: descendants of the radioisotope procedure ancestor
- _Unlike surgery, attribution is by date window, so one radioisotope procedure attaches to every episode whose window contains it. This asymmetry with surgery is deliberate but must be named — see OC-M1._

**`rt_course_mv`**

- multiplies — episode_event: outer join on the procedure_occurrence_id discriminator; several prescription procedures multiply
- multiplies — modified_procedure_mv: outer join; itself at procedure/intent grain
- reduces — fraction_mv: GROUP BY (person, fraction_id, fraction_number, course_id, fraction_name) — collapses to FRACTION grain, not course grain
- _The actual output grain is (fraction_id, course_prescription_id, intent) despite the course naming._

**`sact_treatment_mv`**

- multiplies — episode_event: outer join on the procedure_occurrence_id discriminator; several prescription procedures multiply
- multiplies — modified_procedure_mv: outer join; itself at procedure/intent grain, so several intents multiply again
- reduces — cycle_mv: GROUP BY (person, cycle_id, cycle_number, regimen_id, cycle_concept) — collapses to CYCLE grain, not regimen grain
- _The actual output grain is (cycle_id, regimen_prescription_id, intent) despite the regimen naming. Everything downstream that treats a row as one regimen over-counts._

**`surgical_procedure_mv`**

- multiplies — procedure_occurrence: outer join through episode_event on the procedure_occurrence_id discriminator; several surgeries per episode multiply, which is the intended grain
- reduces — surg_only concept set: descendants of surgical_procedure minus radiotherapy and radioisotope descendants
- _Attribution is explicit Episode_Event linkage, so pipeline-runtime is the authority on which episode owns a surgery. There is no date-window fan-out here._

**`treatment_envelope_mv`**

- multiplies — modified_conditions_mv: spine at condition-occurrence grain; the DISTINCT includes condition_start_date, so several condition records in one episode with different start dates survive as several rows
- reduces — surgical_procedure_mv: pre-aggregated to episode grain by min/max surgery date
- reduces — sact_treatment_mv: pre-aggregated to episode grain by min/max exposure date
- reduces — rt_course_mv: pre-aggregated to episode grain by min/max exposure date; see OC-0-N5, a NULL condition_episode_id here removes RT entirely
- _The three modality windows are each pre-aggregated before joining, so they cannot multiply. The multiplier is the condition-occurrence spine. LEAST/GREATEST rely on PostgreSQL's NULL-skipping behaviour, which is intentional and documented in the query._

**`treatment_regimen_cycle_mv`**

- multiplies — episode (child): outer join on episode_parent_id, restricted to treatment_cycle; several cycles multiply the regimen
- reduces — episode (parent): restricted to treatment_regimen

**`bsa_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.body_size_measurements.bsa, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`creatinine_clearance_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.lab_measurements.creatinine_clearance, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`dtherm_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.proms_numeric.distress_thermometer, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`dx_measurement_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365
- _Explicit and fallback branches are mutually exclusive. Several valid explicit links or several eligible fallback windows intentionally produce several episode-grain rows._

**`dx_observation_mv`**

- multiplies — episode_event: distinct valid explicit links use the observation_id discriminator and suppress fallback for that observation
- multiplies — condition_episode_mv (window): an unlinked observation is retained once for each eligible overlapping episode window
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`dx_procedure_mv`**

- multiplies — episode_event: distinct valid explicit links use the procedure_occurrence_id discriminator and suppress fallback for that procedure
- multiplies — condition_episode_mv (window): an unlinked procedure is retained once for each eligible overlapping episode window
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`dx_visit_mv`**

- multiplies — episode: inner join on person_id only, restricted to episode_of_care; every one of the person's episodes is a candidate
- reduces — provider: drops visits with no provider
- reduces — provider_concept: drops visits whose provider specialty concept is absent
- reduces — proximity ranking: keeps rows where the tier-1 test holds OR rank = 1; retains every tier-1 episode, so this is not a nearest-episode mapping
- _The filter is `episode_prior = 1 OR rank = 1`, so a visit within 180 days of several episodes attaches to all of them. That fan-out is intended; the ordering that decides rank is not correct — see OC-H3._

**`ecog_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.performance_status_measurements.ecog_performance_status, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`egfr_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.lab_measurements.egfr, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`fev1_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.lab_measurements.fev1, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`height_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.body_size_measurements.height, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`smoking_pyh_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.smoking_numeric.pyh, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`weight_change_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.body_size_measurements.weight_change, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`weight_dx_mv`**

- multiplies — episode_event: distinct valid explicit links use the measurement_id discriminator and suppress fallback for that measurement
- multiplies — condition_episode_mv (window): an unlinked measurement is retained once for each eligible overlapping episode window
- reduces — measurement: restricted to measurements_numeric.body_size_measurements.weight, and to rows with no modifier_of_event_id
- reduces — episode_relevant_window: keeps rows with episode_delta_days between -90 and 365

**`all_stage_modifier_mv`**

- reduces — measurement: restricted to the union of the four TNM/group-stage concept sets
- _Unlike the per-stage-type views this one does not rank, so a condition occurrence with several stage measurements contributes several rows. Every consumer must aggregate or rank it explicitly._

**`grade_modifier_mv`**

- reduces — measurement: restricted to the tumor_grade concept set
- reduces — earliest_modifier ranking: rn = 1 by measurement_date per measurement_event_id

**`group_stage_mv`**

- reduces — measurement: restricted to the tnm_group_stage concept set
- reduces — row_number ranking: rn = 1 per measurement_event_id partition

**`laterality_modifier_mv`**

- reduces — measurement: restricted to the laterality modifier concept
- reduces — earliest_modifier ranking: rn = 1 by measurement_date per measurement_event_id

**`m_stage_mv`**

- reduces — measurement: restricted to the tnm_m_stage concept set
- reduces — row_number ranking: rn = 1 per measurement_event_id partition

**`metastatic_disease_modifier_mv`**

- reduces — measurement: restricted to the metastatic_disease concept set
- reduces — earliest_modifier ranking: rn = 1 by measurement_date per measurement_event_id

**`modified_conditions_mv`**

- multiplies — episode_event: outer join on the condition_occurrence_id discriminator; a condition linked to several episodes multiplies
- reduces — condition_concept: drops conditions whose concept is absent
- _The eight modifier joins are each safe because the modifier views rank to one row per modified event. The only multiplier is episode_event._

**`modified_procedure_mv`**

- multiplies — measurement: outer join on the procedure_occurrence_id modifier field, restricted to treatment_intent concepts; several intents multiply the procedure
- reduces — procedure_concept: drops procedures whose procedure_concept_id is absent from the deployed vocabulary

**`n_stage_mv`**

- reduces — measurement: restricted to the tnm_n_stage concept set
- reduces — row_number ranking: rn = 1 per measurement_event_id partition

**`size_modifier_mv`**

- reduces — measurement: restricted to the tumor_size modifier concept
- reduces — earliest_modifier ranking: rn = 1 by measurement_date per measurement_event_id

**`stage_modifier_mv`**

- multiplies — episode_event: outer join on the condition_occurrence_id discriminator; a condition linked to several episodes multiplies
- multiplies — all_stage_modifier_mv: outer join by measurement_event_id; several stage measurements multiply
- reduces — condition_concept: drops conditions whose concept is absent
- reduces — stage_concept: see OC-0-N3: this inner join defeats the outer join to all_stage_modifier_mv

**`t_stage_mv`**

- reduces — measurement: restricted to the tnm_t_stage concept set
- reduces — row_number ranking: rn = 1 per measurement_event_id partition
- _The ranking guarantees at most one row per measurement_event_id, which is why the modifier views can be left-joined to Condition_Occurrence without multiplying it. See OC-0-N1 for what that partition loses._

### Findings register

`OC-B*`, `OC-H*`, and `OC-M*` are the joint review's own numbering.
`OC-0-N*` were found while cataloguing grains for OC-0.

| Finding | Severity | Scope | Summary | Constructs |
|---|---|---|---|---|
| `OC-0-N1` | high | construct | Modifier ranking partitions on measurement_event_id alone. | 8 |
| `OC-0-N10` | medium | package | Compiled construct SQL is not reproducible: embedded concept-ID IN lists render in hash-seed-dependent set order. | — |
| `OC-0-N2` | high | construct | cycle_mv inner-joins route_concept, silently dropping drug exposures with no resolvable route. | 1 |
| `OC-0-N3` | medium | construct | stage_modifier_mv inner-joins stage_concept onto a left-joined modifier, converting the modifier join to an inner join. | 1 |
| `OC-0-N4` | high | construct | episode_treatment_mv has no executable complete logical key: the intent measurement identity is not projected. | 2 |
| `OC-0-N5` | blocker | construct | rt_course_mv resolves the course episode with the treatment_regimen concept, not radiotherapy. | 1 |
| `OC-0-N6` | medium | construct | condition_treatment_episode_mv regimen_count and course_count are always 1. | 1 |
| `OC-0-N7` | low | construct | condition_treatment_episode_mv materialises four duplicate unmapped columns. | 1 |
| `OC-0-N8` | medium | construct | sact_treatment_mv and rt_course_mv read modified_procedure_mv without declaring it. | 2 |
| `OC-0-N9` | medium | package | ConstructRegistry.validate() reads information_schema.columns, which does not list materialized views. | — |
| `OC-B3` | blocker | construct | ORM primary keys contradict multi-row view grains, so SQLAlchemy's identity map collapses child-specific values. | 3 |
| `OC-B4` | blocker | construct | Demography postcode, country-of-birth, and language subqueries are unranked, producing an N x M x K product per person/episode. | 1 |
| `OC-B5` | blocker | package | Materialized-view lifecycle operations are unqualified, so they depend on search_path rather than the inspected schema. | — |
| `OC-H1` | high | construct | Treatment queries carry undocumented many-to-many multipliers; regimen and course views are really cycle- and fraction-grain. | 7 |
| `OC-H2` | high | package | Concept resolution performs import-time database I/O and duplicates omop-alchemy. | — |
| `OC-H3` | high | construct | Visit-to-episode ranking orders by signed delta rather than absolute proximity, and the first tier excludes the exact 180-day boundary. | 1 |
| `OC-H4` | high | construct | Treatment modality intent inspects only events linked directly to the intent episode, omitting events on child episodes. | 2 |
| `OC-M1` | medium | construct | Surgery attribution is explicit-link plus null spine while the catalogue described date windows; the radioisotope path still uses date windows. | 2 |
| `OC-M2` | medium | package | Standard-concept mapping mislabels vocabulary_id and collapses one-to-many 'Maps to' relationships. | — |
| `OC-M3` | medium | construct | Public identifiers are frequently unordered row_number() surrogates: unique within one materialization, not stable across refreshes. | 30 |
| `OC-M4` | medium | package | Repeated mapper-registration warnings hide genuine mapping conflicts. | — |

<!-- END GENERATED: construct-contracts -->
