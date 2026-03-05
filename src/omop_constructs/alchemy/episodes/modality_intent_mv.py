import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .modality_intent_join import episode_join

@register_construct
class EpisodeTreatmentMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "episode_treatment_mv"
    __mv_select__ = episode_join.select()
    __mv_index__ = "episode_id"
    __deps__ = ()

    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)

    episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    episode_end_date: so.Mapped[date] = so.mapped_column(sa.Date)

    measurement_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    concept_name: so.Mapped[str] = so.mapped_column(sa.String)

    rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    sact: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    concurrent: so.Mapped[bool] = so.mapped_column(sa.Boolean)