import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import List

from omop_alchemy.cdm.model.structural import Episode_Event
from ..modifiers import ModifiedCondition
from .condition_episode_mv import ConditionEpisodeMV   # wherever you defined it
from omop_semantics.runtime.default_valuesets import runtime # type: ignore


class ConditionEpisodeObject(ConditionEpisodeMV):
    """
    Consumption-layer wrapper around ConditionEpisodeMV that exposes
    relationships to Episode_Event and resolved Condition_Occurrence rows.
    """

    __tablename__ = ConditionEpisodeMV.__tablename__
    __mapper_args__ = {"concrete": False}
    # All Episode_Event rows for this episode
    episode_events: so.Mapped[List[Episode_Event]] = so.relationship(
        Episode_Event,
        primaryjoin=ConditionEpisodeMV.disease_episode_id == so.foreign(Episode_Event.episode_id),
        viewonly=True,
        lazy="selectin",
    )

    # Only Condition_Occurrence-linked events
    condition_events: so.Mapped[List[Episode_Event]] = so.relationship(
        Episode_Event,
        primaryjoin=sa.and_(
            ConditionEpisodeMV.disease_episode_id == so.foreign(Episode_Event.episode_id),
            Episode_Event.episode_event_field_concept_id
            == runtime.modifiers.modifier_fields.condition_occurrence_id,
        ),
        viewonly=True,
        lazy="selectin",
    )

    # Direct relationship to ModifiedCondition MV via Episode_Event spine
    modified_conditions: so.Mapped[List[ModifiedCondition]] = so.relationship(
        ModifiedCondition,
        secondary=Episode_Event.__table__,
        primaryjoin=ConditionEpisodeMV.disease_episode_id == so.foreign(Episode_Event.episode_id),
        secondaryjoin=sa.and_(
            so.foreign(Episode_Event.event_id) == ModifiedCondition.condition_occurrence_id,
            Episode_Event.episode_event_field_concept_id
            == runtime.modifiers.modifier_fields.condition_occurrence_id,
        ),
        viewonly=True,
        lazy="selectin",
    )
