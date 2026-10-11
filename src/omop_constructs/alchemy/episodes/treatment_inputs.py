"""Private source guards shared by the treatment consumers of procedure modifiers.

The raw modifier MV remains unchanged. A foreign-person modifier cannot suppress
the underlying procedure: the outer join retains a null-intent procedure instead.
"""

import sqlalchemy as sa
from omop_alchemy.cdm.model import Concept, Episode, Episode_Event, Measurement, Procedure_Occurrence
from omop_semantics.runtime.default_valuesets import runtime

from ..modifiers.procedure_modifier_mv import ModifiedProcedure
from .episode_joins import treatment_attribution


# Carry the source episode link into each consumer instead of joining the same
# Episode_Event table twice. Consumers deduplicate after treatment/ownership
# joins, allowing PostgreSQL to reorder those joins and narrow clinical rows.
linked_procedures = (
    sa.select(Episode_Event.episode_id, Episode_Event.event_id)
    .where(Episode_Event.episode_event_field_concept_id == runtime.modifiers.modifier_fields.procedure_occurrence_id)
    .subquery("linked_treatment_procedures")
)

# Validate intents independently of the procedure outer join. Invalid modifier
# rows disappear here; the outer join below still supplies a null-intent row
# when a procedure has no valid modifier, including foreign-person intents.
valid_modifiers = (
    sa.select(ModifiedProcedure)
    .outerjoin(Measurement, sa.and_(
        Measurement.measurement_id == ModifiedProcedure.intent_id,
        Measurement.person_id == ModifiedProcedure.person_id,
    ))
    .where(sa.or_(ModifiedProcedure.intent_id.is_(None), Measurement.measurement_id.is_not(None)))
    .subquery("valid_treatment_modifiers")
)

guarded_procedures = (
    sa.select(
        linked_procedures.c.episode_id,
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_datetime,
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_concept_id,
        Concept.concept_name.label("procedure_concept"),
        valid_modifiers.c.intent_id,
        valid_modifiers.c.intent_concept,
        valid_modifiers.c.intent_concept_id,
        valid_modifiers.c.intent_datetime,
    )
    .join(linked_procedures, linked_procedures.c.event_id == Procedure_Occurrence.procedure_occurrence_id)
    .join(Concept, Concept.concept_id == Procedure_Occurrence.procedure_concept_id)
    .outerjoin(valid_modifiers, sa.and_(
        valid_modifiers.c.procedure_occurrence_id == Procedure_Occurrence.procedure_occurrence_id,
        valid_modifiers.c.person_id == Procedure_Occurrence.person_id,
    ))
    .subquery("guarded_treatment_procedures")
)

prescription_links = (
    sa.select(*guarded_procedures.c)
    .join(Episode, sa.and_(
        Episode.episode_id == guarded_procedures.c.episode_id,
        sa.case((Episode.person_id == guarded_procedures.c.person_id, True), else_=False),
        Episode.episode_concept_id == runtime.types.treatment_episode_types.treatment_regimen,
    ))
    .distinct()
    .subquery("treatment_prescriptions")
)


def treatment_detail(activity: sa.Subquery, group: str) -> sa.Select:
    """Attribute cycle/fraction aggregates without multiplying physical counts."""
    detail = (
        sa.select(
            *activity.c,
            Episode.episode_number.label(f"{group}_number"),
            treatment_attribution.c.target_id.label("condition_episode_id"),
            prescription_links.c.procedure_occurrence_id.label(f"{group}_prescription_id"),
            prescription_links.c.procedure_concept_id.label(f"{group}_concept_id"),
            prescription_links.c.procedure_concept.label(f"{group}_concept"),
            prescription_links.c.intent_concept,
            prescription_links.c.intent_concept_id,
            prescription_links.c.intent_datetime,
            prescription_links.c.intent_id,
        )
        .join(Episode, sa.and_(Episode.episode_id == activity.c[f"{group}_id"],
                              sa.case((Episode.person_id == activity.c.person_id, True), else_=False),
                              Episode.episode_concept_id == runtime.types.treatment_episode_types.treatment_regimen))
        .join(treatment_attribution, sa.and_(
            treatment_attribution.c.owner_id == Episode.episode_parent_id,
            # Person and episode identity are correlated. Keep the person guard
            # as a residual predicate so PostgreSQL does not estimate them as
            # independent join keys and choose a repeated scan of attribution.
            sa.case((treatment_attribution.c.person_id == Episode.person_id, True), else_=False),
        ))
        # prescription_links already validates person against this globally
        # unique episode ID; repeating that key misestimates the join at scale.
        .outerjoin(prescription_links, prescription_links.c.episode_id == Episode.episode_id)
        .distinct()
        .subquery(f"attributed_{group}")
    )
    return sa.select(sa.func.row_number().over().label("mv_id"), *detail.c)
