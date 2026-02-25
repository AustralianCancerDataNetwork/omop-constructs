import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional
from orm_loader.helpers import Base

from omop_constructs.alchemy.episodes.condition_episode_mv import ConditionEpisodeMV
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from ..episodes import ConditionEpisodeMV
from .event_queries import (
    weight_change_dx,
    weight_dx,
    height_dx,
    bsa_dx,
    creat_dx,
    egfr_dx,
    fev1_dx,
    dtherm_dx,
    ecog_dx,
    pyh_dx,
    dx_all_measurements
)

class ConditionEpisodeMeasurementCols:
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_date: so.Mapped[date] = so.mapped_column(sa.Date)
    event_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    event_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    value_as_number: so.Mapped[Optional[float]] = so.mapped_column(sa.Float, nullable=True)
    unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)

    episode_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    episode_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    episode_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

    episode_delta_days: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)


@register_construct
class WeightDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "weight_dx_mv"
    __mv_select__ = weight_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__


@register_construct
class WeightChangeDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "weight_change_dx_mv"
    __mv_select__ = weight_change_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__


@register_construct
class HeightDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "height_dx_mv"
    __mv_select__ = height_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__


@register_construct
class BSADxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "bsa_dx_mv"
    __mv_select__ = bsa_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class CreatinineClearanceDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "creatinine_clearance_dx_mv"
    __mv_select__ = creat_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class EGFRDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "egfr_dx_mv"
    __mv_select__ = egfr_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__ 


@register_construct
class FEV1DxMV( 
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "fev1_dx_mv"
    __mv_select__ = fev1_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class DistressThermometerDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "dtherm_dx_mv"
    __mv_select__ = dtherm_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class ECOGDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "ecog_dx_mv"
    __mv_select__ = ecog_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class SmokingPYHDxMV(
    ConditionEpisodeMeasurementCols,
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "smoking_pyh_dx_mv"
    __mv_select__ = pyh_dx.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__

@register_construct
class DxMeasurementMV(ConditionEpisodeMeasurementCols, MaterializedViewMixin, Base):
    # todo: the other measurement MVs could be slices of this one instead of 
    # re-creating from fresh joins every time...
    __mv_name__ = "dx_measurement_mv"
    __mv_select__ = dx_all_measurements.select()
    __mv_index__ = "person_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__,)
    __tablename__ = __mv_name__
    value_as_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
