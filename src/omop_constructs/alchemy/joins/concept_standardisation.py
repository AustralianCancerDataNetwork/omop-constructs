from orm_loader.helpers import Base
from collections import defaultdict
import sqlalchemy.orm as so
import sqlalchemy as sa
from omop_alchemy.cdm.model.vocabulary import Concept, Concept_Relationship

source_concept = so.aliased(Concept, name='source_concept')
standard_concept = so.aliased(Concept, name='standard_concept')

standard_concept_query = (
    sa.select(
        source_concept.concept_id.label('source_concept_id'), 
        source_concept.concept_code.label('source_code'), 
        source_concept.concept_name.label('source_name'), 
        source_concept.concept_name.label('source_vocab'), 
        standard_concept.concept_id.label('standard_concept_id'), 
        standard_concept.vocabulary_id.label('standard_vocab'), 
        standard_concept.concept_code.label('standard_code'), 
        standard_concept.concept_name.label('standard_name')
    )
    .join(Concept_Relationship, source_concept.concept_id == Concept_Relationship.concept_id_1)
    .join(standard_concept, standard_concept.concept_id == Concept_Relationship.concept_id_2)
    .filter(Concept_Relationship.relationship_id=='Maps to')
    .subquery()
)

class Standard_Concept_Mapper(Base):
    __table__ = standard_concept_query
    __mapper_args__ = {
        "primary_key": [
            standard_concept_query.c.source_concept_id,
            standard_concept_query.c.standard_concept_id,
        ]
    }
    source_concept_id: so.Mapped[int] 
    source_code: so.Mapped[str] 
    source_name: so.Mapped[str] 
    source_vocab: so.Mapped[str] 
    standard_concept_id: so.Mapped[int] 
    standard_vocab: so.Mapped[str] 
    standard_code: so.Mapped[str] 
    standard_name: so.Mapped[str] 
