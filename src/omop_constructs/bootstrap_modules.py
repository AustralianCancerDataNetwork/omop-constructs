"""
Declarative manifest of construct-defining modules.

The tuples in this module are intentionally simple data rather than loader
functions. Keeping the manifest separate from import execution makes the full
registry bootstrap path easier to inspect, test, and reuse.
"""

from __future__ import annotations

MODIFIER_CONSTRUCT_MODULES: tuple[str, ...] = (
    "omop_constructs.alchemy.modifiers.modifier_mappers",
    "omop_constructs.alchemy.modifiers.condition_modifier_mv",
    "omop_constructs.alchemy.modifiers.procedure_modifier_mv",
)
"""Construct modules that make up the modifier family."""

EPISODE_CONSTRUCT_MODULES: tuple[str, ...] = (
    "omop_constructs.alchemy.episodes.condition_episode_mv",
    "omop_constructs.alchemy.episodes.surgical_procedure_mv",
    "omop_constructs.alchemy.episodes.systemic_treatment_mv",
    "omop_constructs.alchemy.episodes.cycle_mv",
    "omop_constructs.alchemy.episodes.course_mv",
    "omop_constructs.alchemy.episodes.fraction_mv",
    "omop_constructs.alchemy.episodes.treatment_summary_mv",
    "omop_constructs.alchemy.episodes.treatment_envelope_mv",
    "omop_constructs.alchemy.episodes.modality_intent_mv",
    "omop_constructs.alchemy.episodes.consult_window_mv",
)
"""Construct modules that make up the episode family."""

EVENT_CONSTRUCT_MODULES: tuple[str, ...] = (
    "omop_constructs.alchemy.events.dx_linked_event_mv",
    "omop_constructs.alchemy.events.dx_linked_obs_mv",
    "omop_constructs.alchemy.events.dx_linked_procedure_mv",
    "omop_constructs.alchemy.events.dx_linked_visit_mv",
)
"""Construct modules that make up the event family."""

DEMOGRAPHY_CONSTRUCT_MODULES: tuple[str, ...] = (
    "omop_constructs.alchemy.demography.demography_matview",
)
"""Construct modules that make up the demography family."""

CDM_CONSTRUCT_MODULES: tuple[str, ...] = (
    *MODIFIER_CONSTRUCT_MODULES,
    *EPISODE_CONSTRUCT_MODULES,
    *EVENT_CONSTRUCT_MODULES,
    *DEMOGRAPHY_CONSTRUCT_MODULES,
)
"""Full ordered manifest for loading the shipped construct registry."""
