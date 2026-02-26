import sqlalchemy as sa
from orm_loader.helpers import Base
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct
from .procedure_modifier_joins import modified_procedure_join

@register_construct
class ModifiedProcedure(MaterializedViewMixin, Base):
    """
    this class contains all stage modifier records no matter the sub-type, nor does it preference 
    clin / path stage etc - just a dump of all, along with the identifier to link to condition ep
    """
    __mv_name__ = 'modified_procedure_mv'
    __mv_select__ = modified_procedure_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__
    __deps__ = ()
    mv_id = sa.Column(sa.Integer,primary_key=True)
    person_id = sa.Column(sa.Integer)
    procedure_datetime = sa.Column(sa.Date)
    procedure_occurrence_id = sa.Column(sa.Integer)
    procedure_source_value = sa.Column(sa.String)
    procedure_concept_id = sa.Column(sa.Integer)
    procedure_concept = sa.Column(sa.String)
    intent_id = sa.Column(sa.Integer)
    intent_datetime = sa.Column(sa.Date)
    intent_concept_id = sa.Column(sa.Integer)
    intent_concept = sa.Column(sa.String)
    stage_label = sa.Column(sa.String)
