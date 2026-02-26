import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from .fraction_joins import fraction_join
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class FractionMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "fraction_mv"
    __mv_select__ = fraction_join.select()
    __mv_index__ = "fraction_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    procedure_occurrence_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    procedure_datetime: so.Mapped[date] = so.mapped_column(sa.DateTime)
    procedure_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    procedure_concept: so.Mapped[str] = so.mapped_column(sa.String)
    intent_datetime: so.Mapped[date] = so.mapped_column(sa.DateTime)
    intent_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    intent_concept: so.Mapped[str] = so.mapped_column(sa.String)
    fraction_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    course_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    fraction_name: so.Mapped[str] = so.mapped_column(sa.String)