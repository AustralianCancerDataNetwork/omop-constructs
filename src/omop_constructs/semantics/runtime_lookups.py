
from omop_alchemy import get_engine_name, load_environment
from omop_alchemy.cdm.handlers import ConceptResolverRegistry
from omop_semantics.runtime.default_valuesets import runtime
import sqlalchemy as sa
import os

from .lookups import get_concept_resolver_registry, build_stage_resolver, build_parent_resolver

DEFAULT_RESOLVER_BUILDERS = {
    "tnm_t_stage": lambda session: build_stage_resolver(
        session,
        parent_list=list(runtime.staging.t_stage_concepts.ids),
        stage_name="t",
    ),
    "tnm_n_stage": lambda session: build_stage_resolver(
        session,
        parent_list=list(runtime.staging.n_stage_concepts.ids),
        stage_name="n",
    ),
    "tnm_m_stage": lambda session: build_stage_resolver(
        session,
        parent_list=list(runtime.staging.m_stage_concepts.ids),
        stage_name="m",
    ),
    "tnm_group_stage": lambda session: build_stage_resolver(
        session,
        parent_list=list(runtime.staging.group_stage_concepts.ids),
        stage_name="group",
    ),
    "metastatic_disease": lambda session: build_parent_resolver(
        session,
        parent_list=[runtime.condition_modifiers.condition_modifier_values.metastatic_disease], # type: ignore
        resolver_name="metastatic_disease",
    ),  
    "tumor_grade": lambda session: build_parent_resolver(
        session,
        parent_list=list(runtime.condition_modifiers.tumor_grade.ids),
        resolver_name="tumor_grade",
    ),
}

def get_registry_engine(env_path: str = __file__):
    if os.environ.get("ENGINE") is None:
        load_environment(env_path)
    engine_string = get_engine_name()
    return sa.create_engine(engine_string, future=True, echo=False)

def get_runtime_resolvers(engine: sa.Engine) -> ConceptResolverRegistry:
    resolver_registry = get_concept_resolver_registry(engine)

    # Register all known resolvers up-front (lazy build)
    for name, builder in DEFAULT_RESOLVER_BUILDERS.items():
        resolver_registry.register(name, builder)

    return resolver_registry