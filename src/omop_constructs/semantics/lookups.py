from omop_alchemy.cdm.handlers import make_concept_resolver, ConceptResolverRegistry
from omop_alchemy.cdm.handlers.vocabs_and_mappers import ConceptResolver, strip_uicc, make_stage
from omop_semantics.runtime.default_valuesets import runtime
from omop_semantics.runtime.unknown_handlers import UNKNOWN
import sqlalchemy.orm as so
import sqlalchemy as sa
from typing import Callable


# for specific target constructs may parent concepts 
# and/or exhaustive lists of concepts to resolve against
# it will depend on how they are used downstream as 
# to whether it is better to resolve at runtime or 
# as a pre-processing step

def get_concept_resolver_registry(engine: sa.engine.Engine) -> ConceptResolverRegistry:
    return ConceptResolverRegistry(engine)



def build_parent_resolver(
        session: so.Session, 
        parent_list: list[int], 
        resolver_name: str, 
        corrections: list[Callable] = [],
        unknown_concept_id: int = UNKNOWN["generic"].concept_id
) -> ConceptResolver:
    return make_concept_resolver(
        session,
        name=resolver_name,
        unknown=unknown_concept_id,
        parents=parent_list,
        include_synonyms=True,
        corrections=corrections,
    )


def build_stage_resolver(
        session: so.Session, 
        parent_list: list[int], 
        stage_name: str
) -> ConceptResolver:
    return build_parent_resolver(
        session,
        resolver_name=f"tnm_{stage_name}_stage",
        parent_list=parent_list,
        corrections=[strip_uicc, make_stage],
    )