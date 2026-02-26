import sqlalchemy as sa
from orm_loader.helpers import Base
from .condition_modifier_join import modified_conditions_join,  all_stage_join
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .modifier_mappers import (
    TStageMV, 
    NStageMV, 
    MStageMV,   
    GroupStageMV,
    GradeModifierMV,
    SizeModifierMV,
    LateralityModifierMV,
    MetastaticDiseaseModifierMV,
    AllStageModifierMV
)

@register_construct
class StageModifier(MaterializedViewMixin, Base):
    """
    this class contains all stage modifier records no matter the sub-type, nor does it preference 
    clin / path stage etc - just a dump of all, along with the identifier to link to condition ep
    """
    __mv_name__ = 'stage_modifier_mv'
    __mv_select__ = all_stage_join.select()
    __mv_pk__ = ["stage_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    __deps__ = (
        AllStageModifierMV.__mv_name__,
     )
    mv_id = sa.Column(sa.Integer, primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_code = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)
    stage_id = sa.Column(sa.Integer)
    stage_date = sa.Column(sa.Date)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)

@register_construct
class ModifiedCondition(MaterializedViewMixin, Base):
    __mv_name__ = 'modified_conditions_mv'
    __mv_select__ = modified_conditions_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    __deps__ = (
        TStageMV.__mv_name__,
        NStageMV.__mv_name__,
        MStageMV.__mv_name__,
        GroupStageMV.__mv_name__,
        GradeModifierMV.__mv_name__,
        SizeModifierMV.__mv_name__,
        LateralityModifierMV.__mv_name__,
        MetastaticDiseaseModifierMV.__mv_name__
    )

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    condition_occurrence_id = sa.Column(sa.Integer)
    condition_source_value = sa.Column(sa.String)
    condition_concept_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_episode = sa.Column(sa.Integer)
    t_stage_id = sa.Column(sa.Integer)
    t_stage_date = sa.Column(sa.Date)
    t_stage_concept_id = sa.Column(sa.Integer)
    t_stage_label = sa.Column(sa.String)
    n_stage_id = sa.Column(sa.Integer)
    n_stage_date = sa.Column(sa.Date)
    n_stage_concept_id = sa.Column(sa.Integer)
    n_stage_label = sa.Column(sa.String)
    m_stage_id = sa.Column(sa.Integer)
    m_stage_date = sa.Column(sa.Date)
    m_stage_concept_id = sa.Column(sa.Integer)
    m_stage_label = sa.Column(sa.String)
    group_stage_id = sa.Column(sa.Integer)
    group_stage_date = sa.Column(sa.Date)
    group_stage_concept_id = sa.Column(sa.Integer)
    group_stage_label = sa.Column(sa.String)
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
    metastatic_disease_id = sa.Column(sa.Integer)
    metastatic_disease_date = sa.Column(sa.Date)
    metastatic_disease_concept_id = sa.Column(sa.Integer)
    metastatic_disease_label = sa.Column(sa.String)