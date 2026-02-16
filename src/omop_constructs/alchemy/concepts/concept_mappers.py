from orm_loader.helpers import Base
from .concept_standardisation import standard_concept_query
import sqlalchemy.orm as so


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
