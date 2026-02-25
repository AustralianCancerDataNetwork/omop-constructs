import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional
from orm_loader.helpers import Base

from omop_constructs.alchemy.episodes.condition_episode_mv import ConditionEpisodeMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from ..episodes import ConditionEpisodeMV
from .procedure_queries import (
    dx_all_procedures
)

class ConditionEpisodeProcedureCols:
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_date: so.Mapped[date] = so.mapped_column(sa.Date)
    event_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    episode_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    episode_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

    episode_delta_days: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)

@register_construct
class DxProcedureMV(ConditionEpisodeProcedureCols, MaterializedViewMixin, Base):
    __mv_name__ = "dx_procedure_mv"
    __mv_select__ = dx_all_procedures.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

    procedure_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    procedure_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime)