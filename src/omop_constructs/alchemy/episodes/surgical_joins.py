import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model import Concept, Concept_Ancestor, Procedure_Occurrence, Observation
from omop_semantics.runtime.default_valuesets import runtime # type: ignore
from .condition_episode_mv import ConditionEpisodeMV
from ..events.event_factories import (
    DEFAULT_EPISODE_WINDOW_DAYS_POST,
    DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
    DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS,
)

# ---------------------------------------------------------------------------
# Concept sets
# ---------------------------------------------------------------------------
# Surgery is defined as any descendant of the surgical_procedure ancestor,
# minus any concept that is also a radiotherapy or radioisotope procedure
# (those overlap in the vocabulary hierarchy and are handled by their own
# constructs).

rth_ca = so.aliased(Concept_Ancestor, name="rth_ca")
srg_ca = so.aliased(Concept_Ancestor, name="srg_ca")

surgical_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(srg_ca, Concept.concept_id == srg_ca.descendant_concept_id)
    .where(srg_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.surgical_procedure)
)

radiotherapy_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .where(rth_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.rt_procedure)
)

radioisotope_concepts = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        Concept.concept_code,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .where(rth_ca.ancestor_concept_id == runtime.cancer_procedures.cancer_procedure_types.rn_procedure)
)

# Exclude RT and radioisotope concepts from the surgical set to prevent
# concepts that appear in multiple branches of the hierarchy from being
# double-counted or misclassified as surgery.
surg_only = surgical_concepts.except_all(
    radiotherapy_concepts.union_all(radioisotope_concepts)
).subquery(name="surg_only")

radioisotopes_only = radioisotope_concepts.subquery(name="radioisotopes_only")


# ---------------------------------------------------------------------------
# Surgery event streams
# ---------------------------------------------------------------------------
# There are two possible sources of surgical information in the CDM:
#
#   1. surgical_procedure_events — Procedure_Occurrence records sourced from
#      the Mosaiq careplan (pipeline step 08_surgical_procedures). These
#      represent procedures actually performed and recorded against the
#      patient's active treatment plan. They carry a real procedure_datetime.
#
#   2. historical_surgical_events — Observation records flagged with the
#      historical_procedure concept. These represent surgical history reported
#      by or about the patient (e.g. prior nephrectomy noted at consultation).
#      The observation_datetime is the date the observation was RECORDED, not
#      necessarily the date the historical surgery occurred. They are NOT
#      suitable for treatment timing calculations.
#
# IMPORTANT: Only stream (1) is used in treatment envelope calculations.
# Stream (2) is retained here for reference and potential use in comorbidity
# or surgical history flags, but must NOT be joined to condition episodes for
# the purpose of deriving treatment dates.

surg_obs_concept = so.aliased(Concept, name="surg_obs_concept")

# Stream 1: active surgical procedures from Procedure_Occurrence.
# Note: these records are NOT registered in Episode_Event at this time. 
# Episode attachment must therefore use date windowing rather than 
# explicit Episode_Event joins - TODO: confirm if we push episode linkage
# as its own distinct phase / utility?
surgical_procedure_events = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id.label("surgery_occurrence_id"),
        Procedure_Occurrence.procedure_concept_id.label("surgery_concept_id"),
        Procedure_Occurrence.procedure_datetime.label("surgery_datetime"),
        surg_only.c.concept_name.label("surgery_name"),
        surg_only.c.concept_code.label("surgery_concept_code"),
        sa.literal("procedure").label("surgery_source"),
    )
    .join(surg_only, surg_only.c.concept_id == Procedure_Occurrence.procedure_concept_id)
)

# Stream 2: historical surgical events from Observation.
# Retained for reference / future use (e.g. surgical history flags, comorbidity
# constructs). Do NOT use in treatment timing — the observation_datetime here
# reflects the date the history was recorded, not when the surgery occurred.
# Using this in a treatment window would attach a surgery from years ago to
# the current episode simply because it was noted at a consultation near
# episode start.
historical_surgical_events = (
    sa.select(
        Observation.person_id,
        Observation.observation_id.label("surgery_occurrence_id"),
        Observation.value_as_concept_id.label("surgery_concept_id"),
        Observation.observation_datetime.label("surgery_datetime"),
        surg_obs_concept.concept_name.label("surgery_name"),
        surg_obs_concept.concept_code.label("surgery_concept_code"),
        sa.literal("observation").label("surgery_source"),
    )
    .join(surg_obs_concept, surg_obs_concept.concept_id == Observation.value_as_concept_id)
    .where(Observation.observation_concept_id == runtime.cancer_procedures.cancer_procedure_types.historical_procedure)
)

# Combined union of both streams. Available for constructs that need a full
# picture of surgical activity regardless of source (e.g. a "has prior surgery"
# flag). NOT used in treatment_envelope — see cancer_relevant_surg_select below.
all_cancer_relevant_surg = (
    sa.union_all(surgical_procedure_events, historical_surgical_events)
    .subquery(name="all_cancer_relevant_surg")
)


# ---------------------------------------------------------------------------
# Episode-attributed surgery — used by SurgicalProcedureMV
# ---------------------------------------------------------------------------
# Only surgical_procedure_events (stream 1) are attached to condition episodes
# here. historical_surgical_events are excluded because their timestamps do not
# reliably represent the date of surgical treatment within the current episode.
#
# Episode attachment uses a date window because no Episode_Event records exist
# for surgical Procedure_Occurrence rows (confirmed: pipeline step 08 writes
# them directly without episode registration).
#
# Window bounds (shared with event_factories defaults):
#   - Look back  DEFAULT_EPISODE_WINDOW_DAYS_PRIOR  days before episode start
#     to capture surgeries performed just before a formal diagnosis is coded
#     (e.g. diagnostic/staging surgery).
#   - Look forward to the episode end date when one is present, signalling an
#     explicitly closed episode. For open-ended episodes, allow up to
#     DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS days after episode start.
#
# A surgery that falls outside this window for every condition episode the
# patient has will produce no row in SurgicalProcedureMV for that surgery.
# That is the correct behaviour — it means the surgery cannot be attributed
# to any known condition episode.

# surgical_procedure_events is kept as a Select (not a subquery) so it can be
# passed directly to sa.union_all() in all_cancer_relevant_surg above. For the
# join below, SQLAlchemy requires a subquery — so we materialise a private alias
# here. Both refer to the same query; the distinction is purely structural.
_surg_proc_sq = surgical_procedure_events.subquery(name="surgical_procedure_events")

_episode_end_bound = sa.func.coalesce(
    ConditionEpisodeMV.episode_end_date,
    ConditionEpisodeMV.episode_start_date + DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS,
)

# Cast surgery_datetime to Date for window comparison. Procedure_Occurrence
# carries a full timestamp (time of surgery) but episode bounds are stored as
# dates. Casting strips the time component and avoids fractional-day edge
# cases where a surgery at 23:59 on the boundary day would otherwise be
# excluded.
_surgery_date = sa.cast(_surg_proc_sq.c.surgery_datetime, sa.Date)

cancer_relevant_surg_select = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        ConditionEpisodeMV.person_id,
        ConditionEpisodeMV.episode_id.label("condition_episode_id"),
        ConditionEpisodeMV.episode_start_date.label("condition_start_date"),
        _surg_proc_sq.c.surgery_occurrence_id,
        _surg_proc_sq.c.surgery_concept_id,
        _surg_proc_sq.c.surgery_datetime,
        _surg_proc_sq.c.surgery_concept_code,
        _surg_proc_sq.c.surgery_name,
        _surg_proc_sq.c.surgery_source,
    )
    .select_from(ConditionEpisodeMV)
    .join(
        _surg_proc_sq,
        sa.and_(
            _surg_proc_sq.c.person_id == ConditionEpisodeMV.person_id,
            _surgery_date >= ConditionEpisodeMV.episode_start_date - DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
            _surgery_date <= _episode_end_bound,
        ),
        isouter=True,
    )
    .subquery(name="cancer_relevant_surg")
)


# ---------------------------------------------------------------------------
# Radioisotope procedures — separate construct, included here for proximity
# ---------------------------------------------------------------------------
# Radioisotope therapy (e.g. Lu-177, Ra-223) shares the Procedure_Occurrence
# source but is conceptually distinct from surgery. Like surgical_procedure_events
# above, these records are written directly as Procedure_Occurrence rows with no
# Episode_Event registration, so episode attachment uses the same date-window
# approach. The same window bounds apply.

_radioisotope_events = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id.label("ri_occurrence_id"),
        Procedure_Occurrence.procedure_concept_id.label("ri_concept_id"),
        Procedure_Occurrence.procedure_datetime.label("ri_datetime"),
        radioisotopes_only.c.concept_name.label("ri_name"),
        radioisotopes_only.c.concept_code.label("ri_concept_code"),
        sa.literal("radioisotope_procedure").label("ri_source"),
    )
    .join(radioisotopes_only, radioisotopes_only.c.concept_id == Procedure_Occurrence.procedure_concept_id)
).subquery(name="radioisotope_events")

_ri_date = sa.cast(_radioisotope_events.c.ri_datetime, sa.Date)

radioisotope_select = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        ConditionEpisodeMV.person_id,
        ConditionEpisodeMV.episode_id.label("condition_episode_id"),
        ConditionEpisodeMV.episode_start_date.label("condition_start_date"),
        _radioisotope_events.c.ri_occurrence_id,
        _radioisotope_events.c.ri_concept_id,
        _radioisotope_events.c.ri_datetime,
        _radioisotope_events.c.ri_name,
        _radioisotope_events.c.ri_concept_code,
        _radioisotope_events.c.ri_source,
    )
    .select_from(ConditionEpisodeMV)
    .join(
        _radioisotope_events,
        sa.and_(
            _radioisotope_events.c.person_id == ConditionEpisodeMV.person_id,
            _ri_date >= ConditionEpisodeMV.episode_start_date - DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
            _ri_date <= _episode_end_bound,
        ),
        isouter=True,
    )
    .subquery(name="radioisotope_procedure")
)
