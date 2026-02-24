import sqlalchemy as sa

from orm_loader.helpers import Base
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .demography_queries import demographics_join
from ..episodes import ConditionEpisodeMV


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

    mv_id = sa.Column(primary_key=True)

    person_id = sa.Column(sa.Integer)
    episode_id = sa.Column(sa.Integer)
    episode_start_date = sa.Column(sa.Date)

    gender_concept_id = sa.Column(sa.Integer)
    mrn = sa.Column(sa.String)
    sex = sa.Column(sa.String)

    year_of_birth = sa.Column(sa.Integer)
    death_datetime = sa.Column(sa.DateTime)

    language_spoken = sa.Column(sa.String)
    country_of_birth = sa.Column(sa.String)
    post_code = sa.Column(sa.Integer)