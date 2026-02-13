import sqlalchemy.orm as so
from collections import defaultdict
from orm_loader.helpers import get_logger
from ..alchemy.joins.concept_standardisation import Standard_Concept_Mapper

logger = get_logger(__name__)


def get_standard_concept_lookup(sess: so.Session, to_standardise: list[int]) -> dict[int, int]:
    standard_concept_lookup = (
        sess.query(
            Standard_Concept_Mapper.source_concept_id,
            Standard_Concept_Mapper.standard_concept_id 
        )
        .filter(Standard_Concept_Mapper.source_concept_id.in_(to_standardise))
        .all()
    )
    logger.info(f"Found {len(standard_concept_lookup)} standard concept mappings for {len(to_standardise)} concepts to standardise")
    return defaultdict(int, {l[0]: l[1] for l in standard_concept_lookup})
