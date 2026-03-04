import sqlalchemy as sa

from orm_loader.helpers import Base
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .demography_queries import demographics_join
from ..episodes import ConditionEpisodeMV
from sqlalchemy.orm import Mapped, mapped_column
from datetime import date, datetime

@register_construct
class PersonDemography(MaterializedViewMixin, Base):
    """
    Person demographics attached to disease episodes.
    
    One row per (person × condition episode).
    """

    __mv_name__ = "person_demography_mv"
    __mv_select__ = demographics_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    __deps__ = (ConditionEpisodeMV.__mv_name__,)

    mv_id: Mapped[int] = mapped_column(primary_key=True)

    person_id: Mapped[int] = mapped_column(sa.Integer)
    episode_id: Mapped[int] = mapped_column(sa.Integer)
    episode_start_date: Mapped[date | None] = mapped_column(sa.Date)

    gender_concept_id: Mapped[int | None] = mapped_column(sa.Integer)
    mrn: Mapped[str | None] = mapped_column(sa.String)
    sex: Mapped[str | None] = mapped_column(sa.String)

    year_of_birth: Mapped[int | None] = mapped_column(sa.Integer)
    death_datetime: Mapped[datetime | None] = mapped_column(sa.DateTime)

    language_spoken: Mapped[str | None] = mapped_column(sa.String)
    country_of_birth: Mapped[str | None] = mapped_column(sa.String)
    post_code: Mapped[int | None] = mapped_column(sa.Integer)