import sqlalchemy as sa
from orm_loader.helpers import Base
from .condition_modifier_join import modified_conditions_join
from ...core.materialized import MaterializedViewMixin


class ModifiedCondition(MaterializedViewMixin, Base):
    __mv_name__ = 'modified_conditions_mv'
    __mv_select__ = modified_conditions_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)
    stage_id = sa.Column(sa.Integer)
    stage_date = sa.Column(sa.Date)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)
    grade_id = sa.Column(sa.Integer)
    grade_date = sa.Column(sa.Date)
    grade_concept_id = sa.Column(sa.Integer)
    grade_label = sa.Column(sa.String)
    size_id = sa.Column(sa.Integer)
    size_date = sa.Column(sa.Date)
    size_concept_id = sa.Column(sa.Integer)
    size_label = sa.Column(sa.String)
    laterality_id = sa.Column(sa.Integer)
    laterality_date = sa.Column(sa.Date)
    laterality_concept_id = sa.Column(sa.Integer)
    laterality_label = sa.Column(sa.String)