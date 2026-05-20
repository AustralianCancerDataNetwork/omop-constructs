import sqlalchemy as sa
import sqlalchemy.orm as so
from datetime import date, datetime
from typing import Optional

from orm_loader.helpers import Base

from ...core.constructs import register_construct
from ...core.materialized import MaterializedViewMixin
from .visit_queries import dx_relevant_visits


class ConditionEpisodeVisitCols:
    """
    Shared mapped columns for diagnosis-relevant visit materialized views.
    """
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    visit_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    visit_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(
        sa.DateTime, nullable=True
    )

    provider_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    provider_specialty: so.Mapped[Optional[str]] = so.mapped_column(
        sa.String, nullable=True
    )
    provider_specialty_concept_id: so.Mapped[Optional[int]] = so.mapped_column(
        sa.Integer, nullable=True
    )

    episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)

    episode_prior: so.Mapped[int] = so.mapped_column(sa.Integer)
    diff_days: so.Mapped[float] = so.mapped_column(sa.Float)
    rank: so.Mapped[int] = so.mapped_column(sa.Integer)


@register_construct
class DxRelevantVisitMV(ConditionEpisodeVisitCols, MaterializedViewMixin, Base):
    """
    Each row is one visit occurrence assigned to one episode. A visit may appear
    more than once if it falls within 180 days of multiple episodes (episode_prior == 1);
    the ``rank`` column indicates the highest-priority episode assignment for that visit
    (rank=1 is the best match). Multiple visits per episode appear as separate rows —
    there is no within-episode aggregation.

    Each row carries a single atomic specialty concept. Specialty grouping (e.g.
    combining radiation oncology + medical oncology) is the responsibility of
    downstream measure engines; this construct does not group specialties.

    """
    __mv_name__ = "dx_visit_mv"
    __mv_select__ = dx_relevant_visits.select()
    __mv_index__ = "person_id"
    __deps__ = ()
    __tablename__ = __mv_name__
