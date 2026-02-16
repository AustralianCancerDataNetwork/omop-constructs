import sqlalchemy as sa
from orm_loader.helpers import Base
from .modifier_joins import (
    t_stage_select, 
    n_stage_select, 
    m_stage_select, 
    group_stage_select,
    laterality_select,
    size_select,
    grade_select,
)
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

"""
Each of the following mapper classes are guaranteed to return exactly 1 or zero
records per condition occurrence, as all the preferencing logic is handled in 
the underlying queries.

This means we can safely left join these to Condition_Occurrence without worrying 
about duplication, but if you want non-standard resolution of modifiers per
diagnosis, bespoke queries and/or mapper objects will be necessary.
"""

class StageColumns:
    __table_args__ = {"extend_existing": True}
    person_id = sa.Column(sa.Integer)
    stage_id = sa.Column(primary_key=True)
    stage_date = sa.Column(sa.Date)
    stage_datetime = sa.Column(sa.DateTime)
    stage_concept_id = sa.Column(sa.Integer)
    stage_label = sa.Column(sa.String)
    measurement_event_id = sa.Column(sa.Integer)
    meas_event_field_concept_id = sa.Column(sa.Integer)

class MeasModCols:
    __table_args__ = {"extend_existing": True}
    
    person_id = sa.Column(sa.Integer)
    measurement_id = sa.Column(primary_key=True)
    measurement_date = sa.Column(sa.Date)
    concept_name = sa.Column(sa.String)
    measurement_event_id = sa.Column(sa.Integer)
    meas_event_field_concept_id = sa.Column(sa.Integer)


@register_construct
class TStageMV(StageColumns, MaterializedViewMixin, Base):
    __mv_name__ = "t_stage_mv"
    __mv_select__ = t_stage_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__

@register_construct
class NStageMV(StageColumns, MaterializedViewMixin, Base):
    __mv_name__ = "n_stage_mv"
    __mv_select__ = n_stage_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__

@register_construct
class MStageMV(StageColumns, MaterializedViewMixin, Base):
    __mv_name__ = "m_stage_mv"
    __mv_select__ = m_stage_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__

@register_construct
class GroupStageMV(StageColumns, MaterializedViewMixin, Base):
    __mv_name__ = "group_stage_mv"
    __mv_select__ = group_stage_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__

@register_construct
class SizeModifierMV(MeasModCols, MaterializedViewMixin, Base):
    __mv_name__ = "size_modifier_mv"
    __mv_select__ = size_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    unit_concept_id = sa.Column(sa.Integer)

@register_construct
class GradeModifierMV(MeasModCols, MaterializedViewMixin, Base):
    __mv_name__ = "grade_modifier_mv"
    __mv_select__ = grade_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    measurement_concept_id = sa.Column(sa.Integer)

@register_construct
class LateralityModifierMV(MeasModCols, MaterializedViewMixin, Base):
    __mv_name__ = "laterality_modifier_mv"
    __mv_select__ = laterality_select.select()
    __mv_index__ = "measurement_event_id"
    __deps__ = ()
    __tablename__ = __mv_name__
    value_as_concept_id = sa.Column(sa.Integer)

# class TStage(StageColumns, Base):
#     __table__ = t_stage_select
#     __tablename__ = 't_stage'

# class NStage(StageColumns, Base):
#     __table__ = n_stage_select
#     __tablename__ = 'n_stage'
    
# class MStage(StageColumns, Base):
#     __table__ = m_stage_select
#     __tablename__ = 'm_stage'

# class GroupStage(StageColumns, Base):
#     __table__ = group_stage_select
#     __tablename__ = 'group_stage'
    
# class SizeModifier(MeasModCols, Base):
#     __table__ = size_select
#     __tablename__ = 'size_modifier'
#     value_as_number = size_select.c.value_as_number
#     unit_concept_id = size_select.c.unit_concept_id
    
# class GradeModifier(MeasModCols, Base):
#     __table__ = grade_select
#     __tablename__ = 'grade_modifier'
#     measurement_concept_id = grade_select.c.measurement_concept_id

# class LatModifier(MeasModCols, Base):
#     __table__ = laterality_select
#     __tablename__ = 'laterality_modifier'
#     value_as_concept_id = laterality_select.c.value_as_concept_id

