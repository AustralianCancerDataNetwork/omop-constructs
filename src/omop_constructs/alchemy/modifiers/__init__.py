from ..._lazy_imports import load_export

_EXPORTS: dict[str, str] = {
    "TStageMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "NStageMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "MStageMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "GroupStageMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "GradeModifierMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "LateralityModifierMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "SizeModifierMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "MetastaticDiseaseModifierMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "AllStageModifierMV": "omop_constructs.alchemy.modifiers.modifier_mappers",
    "ModifiedCondition": "omop_constructs.alchemy.modifiers.condition_modifier_mv",
    "StageModifier": "omop_constructs.alchemy.modifiers.condition_modifier_mv",
    "ModifiedProcedure": "omop_constructs.alchemy.modifiers.procedure_modifier_mv",
}

__all__ = [
    "TStageMV",
    "NStageMV",
    "MStageMV",
    "GroupStageMV",
    "GradeModifierMV",
    "LateralityModifierMV",
    "SizeModifierMV",
    "ModifiedCondition",
    "MetastaticDiseaseModifierMV",
    "AllStageModifierMV",
    "StageModifier",
    "ModifiedProcedure"
]


def __getattr__(name: str):
    return load_export(name, _EXPORTS)


def __dir__() -> list[str]:
    return sorted(__all__)
