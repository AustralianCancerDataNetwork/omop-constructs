# Modifier constructs

From `omop_constructs.alchemy.modifiers`.

Modifier constructs attach clinical annotations (stage, grade, laterality, size, metastatic status) to condition occurrences and episodes.

- `TStageMV`, `NStageMV`, `MStageMV`, `GroupStageMV` — TNM and group stage modifiers
- `AllStageModifierMV` — combined stage modifier surface
- `GradeModifierMV`, `LateralityModifierMV`, `SizeModifierMV`, `MetastaticDiseaseModifierMV` — additional modifier views
- `StageModifier` — unified stage-oriented materialized view
- `ModifiedCondition` — condition occurrences joined to episode and modifier context; used as the spine of most episode-level constructs
- `ModifiedProcedure` — procedure-level modifier surface; used to resolve regimen and course prescriptions with intent context

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
| [`AllStageModifierMV`](#contract-all_stage_modifier_mv) | stage measurement of any TNM or group-stage type |
| [`GradeModifierMV`](#contract-grade_modifier_mv) | modified event |
| [`GroupStageMV`](#contract-group_stage_mv) | modified event |
| [`LateralityModifierMV`](#contract-laterality_modifier_mv) | modified event |
| [`MetastaticDiseaseModifierMV`](#contract-metastatic_disease_modifier_mv) | modified event |
| [`ModifiedCondition`](#contract-modified_conditions_mv) | condition occurrence + linked condition episode |
| [`ModifiedProcedure`](#contract-modified_procedure_mv) | procedure occurrence + treatment-intent modifier measurement |
| [`MStageMV`](#contract-m_stage_mv) | modified event |
| [`NStageMV`](#contract-n_stage_mv) | modified event |
| [`PrimaryDiagnosisConditionMV`](#contract-primary_diagnosis_condition_mv) | condition occurrence + episode-of-care episode |
| [`SizeModifierMV`](#contract-size_modifier_mv) | modified event |
| [`StageModifier`](#contract-stage_modifier_mv) | condition occurrence + validated linked episode + stage measurement |
| [`TStageMV`](#contract-t_stage_mv) | modified event |

### `AllStageModifierMV` {#contract-all_stage_modifier_mv}

View: `all_stage_modifier_mv`

**Rows:** One row per stage measurement of any TNM or group-stage type. Deliberately unranked: this is the long-form stage stream, not a preferred-stage resolver.

**Unique on:** `measurement_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `GradeModifierMV` {#contract-grade_modifier_mv}

View: `grade_modifier_mv`

**Rows:** One row per modified event: the earliest recorded tumour grade measurement for that event.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `GroupStageMV` {#contract-group_stage_mv}

View: `group_stage_mv`

**Rows:** One row per modified event: the preferred group stage for that event, earliest pathological if present, otherwise earliest clinical.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `LateralityModifierMV` {#contract-laterality_modifier_mv}

View: `laterality_modifier_mv`

**Rows:** One row per modified event: the earliest recorded tumour laterality measurement for that event.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `MetastaticDiseaseModifierMV` {#contract-metastatic_disease_modifier_mv}

View: `metastatic_disease_modifier_mv`

**Rows:** One row per modified event: the earliest recorded metastatic-disease measurement for that event.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `ModifiedCondition` {#contract-modified_conditions_mv}

View: `modified_conditions_mv`

**Rows:** One row per (condition occurrence, linked condition episode), carrying the resolved T/N/M/group stage, grade, size, laterality, and metastatic-disease modifiers as columns. The spine of most episode-level constructs.

**Unique on:** `condition_occurrence_id`, `condition_episode`. `condition_episode` is NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `ModifiedProcedure` {#contract-modified_procedure_mv}

View: `modified_procedure_mv`

**Rows:** One row per (procedure occurrence, treatment-intent modifier measurement). A procedure with no intent modifier contributes one row with the intent columns NULL.

**Unique on:** `procedure_occurrence_id`, `intent_id`. `intent_id` is NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `MStageMV` {#contract-m_stage_mv}

View: `m_stage_mv`

**Rows:** One row per modified event: the preferred M stage for that event, earliest pathological if present, otherwise earliest clinical.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `NStageMV` {#contract-n_stage_mv}

View: `n_stage_mv`

**Rows:** One row per modified event: the preferred N stage for that event, earliest pathological if present, otherwise earliest clinical.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `PrimaryDiagnosisConditionMV` {#contract-primary_diagnosis_condition_mv}

View: `primary_diagnosis_condition_mv`

**Rows:** One row per (condition occurrence, episode-of-care episode). modified_conditions_mv restricted to conditions linked to a top-level episode of care, with the episode start and end dates attached.

**Unique on:** `condition_occurrence_id`, `condition_episode`.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `SizeModifierMV` {#contract-size_modifier_mv}

View: `size_modifier_mv`

**Rows:** One row per modified event: the earliest recorded tumour size measurement for that event.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

### `StageModifier` {#contract-stage_modifier_mv}

View: `stage_modifier_mv`

**Rows:** One row per (condition occurrence, validated linked episode, stage measurement), plus one null-stage row when the condition has no stage evidence. The condition spine is retained even when no episode or stage exists.

**Unique on:** `condition_occurrence_id`, `condition_episode`, `stage_id`. `condition_episode`, `stage_id` are NULL on rows with nothing to link.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

### `TStageMV` {#contract-t_stage_mv}

View: `t_stage_mv`

**Rows:** One row per modified event: the preferred T stage for that event, earliest pathological if present, otherwise earliest clinical.

**Unique on:** `person_id`, `meas_event_field_concept_id`, `measurement_event_id`.

**Row IDs:** Taken from the CDM, so they stay the same across refreshes and are safe to store.

<!-- END GENERATED: construct-contracts -->
