import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from typing import Optional
from .systemic_treatment_joins import regimen_join
from .cycle_mv import CycleMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class SACTRegimenMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "sact_treatment_mv"
    __mv_select__ = regimen_join.select()
    __mv_index__ = "regimen_id"
    __deps__ = (CycleMV.__mv_name__,)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    cycle_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    cycle_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    cycle_concept: so.Mapped[int] = so.mapped_column(sa.Integer)
    exposure_count: so.Mapped[int] = so.mapped_column(sa.Integer)
    first_exposure_date: so.Mapped[date] = so.mapped_column(sa.Date)
    last_exposure_date: so.Mapped[date] = so.mapped_column(sa.Date)
    regimen_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_prescription_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_concept: so.Mapped[str] = so.mapped_column(sa.String)
    intent_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    intent_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    intent_concept: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)
    