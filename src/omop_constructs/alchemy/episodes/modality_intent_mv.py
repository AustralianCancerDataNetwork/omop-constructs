import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .modality_intent_join import episode_join

@register_construct
class TreatmentIntentMV(
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

    treatment_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    treatment_episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    treatment_episode_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    treatment_episode_parent_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    treatment_intent_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    treatment_intent_name: so.Mapped[str] = so.mapped_column(sa.String)

    rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    sact: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    concurrent: so.Mapped[bool] = so.mapped_column(sa.Boolean)


from .condition_episode_mv import ConditionEpisodeMV

episode_summary_select = (
    sa.select(
        ConditionEpisodeMV.episode_id,
        ConditionEpisodeMV.person_id,

        ConditionEpisodeMV.episode_concept_id,
        ConditionEpisodeMV.episode_label,

        ConditionEpisodeMV.episode_start_date,
        ConditionEpisodeMV.episode_end_date,

        TreatmentIntentMV.treatment_episode_id,
        TreatmentIntentMV.treatment_episode_start_date,
        TreatmentIntentMV.treatment_episode_end_date,
        TreatmentIntentMV.treatment_intent_concept_id,
        TreatmentIntentMV.treatment_intent_name,

        sa.func.coalesce(TreatmentIntentMV.rt, False).label("rt"),
        sa.func.coalesce(TreatmentIntentMV.sact, False).label("sact"),
        sa.func.coalesce(TreatmentIntentMV.concurrent, False).label("concurrent"),
    )
    .outerjoin(
        TreatmentIntentMV,
        TreatmentIntentMV.treatment_episode_parent_id
        == ConditionEpisodeMV.episode_id
    )
)

@register_construct
class ConditionTreatmentIntentMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "episode_treatment_mv"
    __mv_select__ = episode_summary_select
    __mv_index__ = "episode_id"
    __deps__ = (ConditionEpisodeMV.__mv_name__, TreatmentIntentMV.__mv_name__)
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)

    episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    episode_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_label: so.Mapped[str] = so.mapped_column(sa.String)

    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    episode_end_date: so.Mapped[date] = so.mapped_column(sa.Date)

    treatment_episode_id: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    treatment_episode_start_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)
    treatment_episode_end_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)
    treatment_intent_concept_id: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    treatment_intent_name: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    sact: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    concurrent: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)