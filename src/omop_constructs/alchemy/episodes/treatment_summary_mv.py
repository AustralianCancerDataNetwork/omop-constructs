import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from typing import Optional
from .treatment_summary_joins import condition_treatment_join
from .course_mv import RTCourseMV
from .systemic_treatment_mv import SACTRegimenMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class ConditionTreatmentEpisode(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "condition_treatment_episode_mv"
    __mv_select__ = condition_treatment_join.select()
    __mv_index__ = "condition_episode_id"
    __deps__ = (RTCourseMV.__mv_name__, SACTRegimenMV.__mv_name__)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_occurrence_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    condition_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    course_concept: so.Mapped[str] = so.mapped_column(sa.String)
    course_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    course_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    rt_intent_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    rt_intent_concept: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_count: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_count: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_concept: so.Mapped[str] = so.mapped_column(sa.String)
    regimen_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    regimen_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    sact_intent_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    sact_intent_concept: so.Mapped[int] = so.mapped_column(sa.Integer)
    exposure_count: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_count: so.Mapped[int] = so.mapped_column(sa.Integer)
