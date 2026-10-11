import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from ...core.sql import select_all_columns
from .modality_intent_join import episode_join
from .treatment_envelope_mv import TreatmentEnvelopeMV
from .condition_episode_mv import ConditionEpisodeMV
from .episode_joins import treatment_attribution

@register_construct
class TreatmentIntentMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "episode_treatment_mv"
    __mv_select__ = select_all_columns(episode_join)
    __mv_index__ = "treatment_episode_id"
    __deps__ = ()

    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)

    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    intent_measurement_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    treatment_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    treatment_episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    treatment_episode_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    treatment_episode_parent_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    treatment_intent_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    treatment_intent_name: so.Mapped[str] = so.mapped_column(sa.String)

    rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    sact: so.Mapped[bool | None] = so.mapped_column(sa.Boolean)
    concurrent: so.Mapped[bool] = so.mapped_column(sa.Boolean)


_attributed_intent = (
    sa.select(*(c for c in TreatmentIntentMV.__table__.c if c.key != "mv_id"),
              treatment_attribution.c.target_id)
    .join(treatment_attribution, sa.and_(
        treatment_attribution.c.owner_id == TreatmentIntentMV.treatment_episode_parent_id,
        treatment_attribution.c.person_id == TreatmentIntentMV.person_id,
    ))
    .distinct().subquery("attributed_intent")
)

_episode_concurrency = (
    sa.select(TreatmentEnvelopeMV.person_id, TreatmentEnvelopeMV.condition_episode,
              sa.func.bool_or(sa.func.coalesce(TreatmentEnvelopeMV.concurrent_chemort, False)).label("concurrent_chemort"))
    .group_by(TreatmentEnvelopeMV.person_id, TreatmentEnvelopeMV.condition_episode)
    .subquery("episode_concurrency")
)

episode_summary_select = (
    sa.select(
        sa.func.row_number().over().label("mv_id"),
        ConditionEpisodeMV.episode_id, ConditionEpisodeMV.person_id,
        ConditionEpisodeMV.episode_concept_id, ConditionEpisodeMV.episode_label,
        ConditionEpisodeMV.episode_start_date, ConditionEpisodeMV.episode_end_date,
        _attributed_intent.c.treatment_episode_id,
        _attributed_intent.c.treatment_episode_start_date,
        _attributed_intent.c.treatment_episode_end_date,
        _attributed_intent.c.treatment_intent_concept_id,
        _attributed_intent.c.treatment_intent_name,
        _attributed_intent.c.intent_measurement_id,
        sa.func.coalesce(_attributed_intent.c.rt, False).label("rt"),
        sa.func.coalesce(_attributed_intent.c.sact, False).label("sact"),
        sa.func.coalesce(_attributed_intent.c.concurrent, False).label("concurrent"),
        sa.and_(sa.func.coalesce(_attributed_intent.c.rt, False),
                sa.func.coalesce(_episode_concurrency.c.concurrent_chemort, False)).label("concurrent_rt"),
    )
    .select_from(ConditionEpisodeMV)
    .outerjoin(_attributed_intent, sa.and_(
        _attributed_intent.c.target_id == ConditionEpisodeMV.episode_id,
        _attributed_intent.c.person_id == ConditionEpisodeMV.person_id,
    ))
    .outerjoin(_episode_concurrency, sa.and_(
        _episode_concurrency.c.condition_episode == ConditionEpisodeMV.episode_id,
        _episode_concurrency.c.person_id == ConditionEpisodeMV.person_id,
    ))
)

@register_construct
class ConditionTreatmentIntentMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "condition_episode_intent_mv"
    __mv_select__ = episode_summary_select
    __mv_index__ = "episode_id"
    __deps__ = (
        ConditionEpisodeMV.__mv_name__,
        TreatmentIntentMV.__mv_name__,
        TreatmentEnvelopeMV.__mv_name__,
    )
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)

    episode_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    episode_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    episode_label: so.Mapped[str] = so.mapped_column(sa.String)

    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    episode_end_date: so.Mapped[date] = so.mapped_column(sa.Date)

    intent_measurement_id: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)

    treatment_episode_id: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    treatment_episode_start_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)
    treatment_episode_end_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)
    treatment_intent_concept_id: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    treatment_intent_name: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    sact: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    concurrent: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    concurrent_rt: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
