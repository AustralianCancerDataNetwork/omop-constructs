import sqlalchemy as sa
from orm_loader.helpers import Base
from .episode_joins import overarching_disease_episode
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

class OverarchingDiseaseEpisodeCols:
    __table_args__ = {"extend_existing": True}

    disease_episode_id = sa.Column(sa.Integer, primary_key=True)
    person_id = sa.Column(sa.Integer)

    disease_episode_concept_id = sa.Column(sa.Integer)
    disease_episode_label = sa.Column(sa.String)

    disease_episode_start_date = sa.Column(sa.Date)
    disease_episode_end_date = sa.Column(sa.Date)

    extent_episode_id = sa.Column(sa.Integer)
    extent_episode_concept_id = sa.Column(sa.Integer)
    extent_episode_label = sa.Column(sa.String)

    extent_start_date = sa.Column(sa.Date)
    extent_end_date = sa.Column(sa.Date)

@register_construct
class OverarchingDiseaseEpisodeMV(
    OverarchingDiseaseEpisodeCols,
    MaterializedViewMixin,
    Base,
):
    """
    Materialized view representing overarching disease episodes
    with optional child disease-extent episodes.

    One row per (episode_of_care, disease_extent_episode?) pair.
    """

    __mv_name__ = "overarching_disease_episode_mv"
    __mv_select__ = overarching_disease_episode.select()
    __mv_index__ = "disease_episode_id"
    __deps__ = ()
    __tablename__ = __mv_name__

    def __repr__(self) -> str:
        if self.has_extent:
            return (
                f"<OverarchingDiseaseEpisode "
                f"{self.disease_episode_id} -> extent {self.extent_episode_id}>"
            )
        return f"<OverarchingDiseaseEpisode {self.disease_episode_id}>"
    
    @property
    def has_extent(self) -> bool:
        return self.extent_episode_id is not None

    @property
    def disease_interval(self):
        return (self.disease_episode_start_date, self.disease_episode_end_date)

    @property
    def extent_interval(self):
        if self.extent_episode_id is None:
            return None
        return (self.extent_start_date, self.extent_end_date)