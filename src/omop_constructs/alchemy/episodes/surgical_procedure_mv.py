import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional
from orm_loader.helpers import Base
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .surgical_joins import all_cancer_relevant_surg, cancer_relevant_surg_select, radioisotope_select
from .condition_episode_mv import ConditionEpisodeMV


@register_construct
class SurgicalProcedureMV(MaterializedViewMixin, Base):
    __mv_name__ = "surgical_procedure_mv"
    __mv_select__ = cancer_relevant_surg_select.select()
    __mv_index__ = "mv_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_start_date: so.Mapped[date] = so.mapped_column(sa.Date)

    surgery_occurrence_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    surgery_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    surgery_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime)

    surgery_name: so.Mapped[str] = so.mapped_column(sa.String)
    surgery_concept_code: so.Mapped[str] = so.mapped_column(sa.String)
    surgery_source: so.Mapped[str] = so.mapped_column(sa.String)


@register_construct
class RadioisotopeMV(MaterializedViewMixin, Base):
    __mv_name__ = "radioisotope_mv"
    __mv_select__ = radioisotope_select.select()
    __mv_index__ = "mv_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    condition_start_date: so.Mapped[date] = so.mapped_column(sa.Date)

    ri_occurrence_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    ri_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    ri_datetime: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime)

    ri_name: so.Mapped[str] = so.mapped_column(sa.String)
    ri_concept_code: so.Mapped[str] = so.mapped_column(sa.String)
    ri_source: so.Mapped[str] = so.mapped_column(sa.String)