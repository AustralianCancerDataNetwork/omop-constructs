import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date
from typing import Optional

from orm_loader.helpers import Base

from ...core.constructs import register_construct
from ...core.materialized import MaterializedViewMixin
from ...core.sql import select_all_columns
from ..events.dx_linked_obs_mv import DxObservationMV
from ..events.dx_linked_visit_mv import DxRelevantVisitMV
from .consult_window_query import consult_window
from .treatment_envelope_mv import TreatmentEnvelopeMV


@register_construct
class ConsultWindowMV(MaterializedViewMixin, Base):
    """
    Episode-of-care consult and referral window summary.

    The materialized view exposes the earliest specialist contact and the
    derived scalar windows currently used downstream for referral timing
    analysis.
    """
    __mv_name__ = "consult_window_mv"
    __mv_select__ = select_all_columns(consult_window)
    __mv_index__ = "episode_id"
    __deps__ = (
        DxObservationMV.__mv_name__,
        DxRelevantVisitMV.__mv_name__,
        TreatmentEnvelopeMV.__mv_name__,
    )
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    episode_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    initial_gp_referral: so.Mapped[Optional[date]] = so.mapped_column(
        sa.Date, nullable=True
    )
    first_specialist: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    first_pall_care: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    first_pall_care_or_treatment: so.Mapped[Optional[date]] = so.mapped_column(
        sa.Date, nullable=True
    )
    earliest_treatment: so.Mapped[Optional[date]] = so.mapped_column(
        sa.Date, nullable=True
    )
    referral_to_specialist: so.Mapped[Optional[float]] = so.mapped_column(
        sa.Float, nullable=True
    )
    referral_to_tx: so.Mapped[Optional[float]] = so.mapped_column(
        sa.Float, nullable=True
    )
