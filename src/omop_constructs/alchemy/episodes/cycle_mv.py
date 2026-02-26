import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from .cycle_join import cycle_join
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class CycleMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "cycle_mv"
    __mv_select__ = cycle_join.select()
    __mv_index__ = "cycle_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    drug_exposure_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    drug_exposure_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    drug_exposure_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    quantity: so.Mapped[float] = so.mapped_column(sa.Numeric)
    drug_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    dose_unit_source_value: so.Mapped[str] = so.mapped_column(sa.String)
    drug_name: so.Mapped[str] = so.mapped_column(sa.String)
    route: so.Mapped[str] = so.mapped_column(sa.String)
    cycle_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    cycle_number: so.Mapped[int] = so.mapped_column(sa.Integer)
    cycle_concept: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_id: so.Mapped[int] = so.mapped_column(sa.Integer)
