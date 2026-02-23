import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from typing import Optional
from .episode_joins import overarching_disease_episode, treatment_regimen_with_cycles
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

@register_construct
class OverarchingDiseaseEpisodeMV(
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
    __table_args__ = {"extend_existing": True}

    disease_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    disease_episode_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    disease_episode_label: so.Mapped[str] = so.mapped_column(sa.String)

    disease_episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    disease_episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

    extent_episode_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    extent_episode_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    extent_episode_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    extent_episode_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    extent_episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

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
        return (self.extent_episode_start_date, self.extent_episode_end_date)
    
@register_construct
class TreatmentRegimenCycleMV(
    MaterializedViewMixin,
    Base,
):
    """
    Materialized view representing treatment regimens
    with optional child treatment cycles.

    One row per (treatment_regimen, treatment_cycle?) pair.
    """

    __mv_name__ = "treatment_regimen_cycle_mv"
    __mv_select__ = treatment_regimen_with_cycles.select()
    __mv_index__ = "regimen_episode_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    regimen_episode_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    regimen_episode_concept_id: so.Mapped[int] = so.mapped_column(sa.Integer)
    regimen_episode_label: so.Mapped[str] = so.mapped_column(sa.String)

    regimen_episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    regimen_episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)

    cycle_episode_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    cycle_episode_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    cycle_episode_label: so.Mapped[Optional[str]] = so.mapped_column(sa.String, nullable=True)

    cycle_episode_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    cycle_episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)


    def __repr__(self) -> str:
        if self.has_cycle:
            return (
                f"<TreatmentRegimenCycle "
                f"{self.regimen_episode_id} -> cycle {self.cycle_episode_id}>"
            )
        return f"<TreatmentRegimenCycle {self.regimen_episode_id}>"
    
    @property
    def has_cycle(self) -> bool:
        return self.cycle_episode_id is not None

    @property
    def regimen_interval(self):
        return (self.regimen_episode_start_date, self.regimen_episode_end_date)

    @property
    def cycle_interval(self):
        if self.cycle_episode_id is None:
            return None
        return (self.cycle_episode_start_date, self.cycle_episode_end_date)