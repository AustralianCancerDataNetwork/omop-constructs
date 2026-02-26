import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from typing import Optional
from .course_joins import course_join
from .fraction_mv import FractionMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class RTCourseMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "rt_course_mv"
    __mv_select__ = course_join.select()
    __mv_index__ = "course_id"
    __deps__ = (FractionMV.__mv_name__,)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_count: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_name: so.Mapped[str] = so.mapped_column(sa.String)
    first_exposure_date: so.Mapped[date] = so.mapped_column(sa.Date)
    last_exposure_date: so.Mapped[date] = so.mapped_column(sa.Date)
    course_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_prescription_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_concept: so.Mapped[str] = so.mapped_column(sa.String)
    intent_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    intent_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    intent_concept: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)
    