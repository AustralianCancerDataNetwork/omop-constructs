# Construct Catalog

This page summarizes the current public construct surface exposed by the repository.

## Episode Constructs

From `omop_constructs.alchemy.episodes`:

- `ConditionEpisodeMV`
  All disease episodes represented as a materialized view
- `OverarchingDiseaseEpisodeMV`
  Episode-of-care rows with optional linked extent episodes
- `TreatmentRegimenCycleMV`
  Treatment regimen rows with optional linked cycle episodes
- `DxTreatStartMV`
  Diagnosis-to-treatment timing summary
- `SurgicalProcedureMV`
  Episode-linked cancer surgical procedures
- `SACTRegimenMV`
  Systemic anti-cancer therapy regimen episodes
- `RTCourseMV`
  Radiotherapy course episodes
- `CycleMV`
  Treatment cycle rows
- `FractionMV`
  Radiotherapy fraction rows
- `ConditionTreatmentEpisode`
  Treatment summary view joining condition episode context to RT and SACT summaries
- `TreatmentEnvelopeMV`
  Earliest/latest treatment plus scalar fields such as `days_from_dx_to_treatment`
- `TreatmentIntentMV`
  Treatment intent events
- `ConditionTreatmentIntentMV`
  Treatment intents attached back to condition episodes
- `ConsultWindowMV`
  Episode-of-care consult and referral window scalars, including `referral_to_specialist` and `referral_to_tx`

## Event Constructs

From `omop_constructs.alchemy.events`:

- `DxMeasurementMV`
  Generic diagnosis-linked measurement surface
- `WeightDxMV`
- `WeightChangeDxMV`
- `HeightDxMV`
- `BSADxMV`
- `CreatinineClearanceDxMV`
- `EGFRDxMV`
- `FEV1DxMV`
- `DistressThermometerDxMV`
- `ECOGDxMV`
- `SmokingPYHDxMV`
  Focused diagnosis-linked measurement slices
- `DxObservationMV`
  Diagnosis-linked observations
- `DxProcedureMV`
  Diagnosis-linked procedures
- `DxRelevantVisitMV`
  episode-linked visits with resolved provider-specialty included.
  Each row is one visit occurrence assigned to one episode, carrying a single
  atomic specialty concept. Multiple visits per episode appear as separate rows;
  no specialty grouping or within-episode aggregation occurs here.


## Modifier Constructs

From `omop_constructs.alchemy.modifiers`:

- `TStageMV`
- `NStageMV`
- `MStageMV`
- `GroupStageMV`
  Stage-specific modifier views
- `AllStageModifierMV`
  Combined stage modifier surface
- `GradeModifierMV`
- `LateralityModifierMV`
- `SizeModifierMV`
- `MetastaticDiseaseModifierMV`
  Additional condition modifier views
- `StageModifier`
  Unified stage-oriented materialized view
- `ModifiedCondition`
  Condition occurrences joined to episode and modifier context
- `ModifiedProcedure`
  Procedure-level modifier surface

## Demography Constructs

From `omop_constructs.alchemy.demography`:

- `PersonDemography`
  Demographic attributes attached to condition episodes

## Condition Constructs

From `omop_constructs.alchemy.conditions`:

- `Condition_Window`
  Mapped condition window query surface

## Supporting Query And Resolver Modules

The following modules are not themselves construct registries but are part of the active public architecture:

- `omop_constructs.core`
  registry, planning, DDL, and materialized view helpers
- `omop_constructs.semantics`
  runtime concept resolvers
- `omop_constructs.alchemy.events.event_factories`
  generic event-to-episode attachment functions
- `omop_constructs.alchemy.episodes.episode_factories`
  reusable episode query builders

## Notes On Catalog Scope

This catalog reflects the construct classes currently exported by the package, not every internal query fragment in the repository.

The highest-level rule of thumb is:

- if it is a mapped class with `__mv_name__`, it belongs in the construct lifecycle
- if it is a query factory or query fragment, it is supporting infrastructure
