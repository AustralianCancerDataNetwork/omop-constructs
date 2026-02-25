import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional
from orm_loader.helpers import Base

from omop_constructs.alchemy.episodes.condition_episode_mv import ConditionEpisodeMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from ..episodes import ConditionEpisodeMV
from .observation_queries import (
    dx_all_observations
)

class ConditionEpisodeObservationCols:
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_date: so.Mapped[date] = so.mapped_column(sa.Date)
    event_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    value_as_number: so.Mapped[Optional[float]] = so.mapped_column(sa.Float, nullable=True)
    qualifier_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    value_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)

    episode_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    episode_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

    episode_delta_days: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)


@register_construct
class DxObservationMV(ConditionEpisodeObservationCols, MaterializedViewMixin, Base):
    __mv_name__ = "dx_observation_mv"
    __mv_select__ = dx_all_observations.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

    observation_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    observation_date: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime)
    value_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    qualifier_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)