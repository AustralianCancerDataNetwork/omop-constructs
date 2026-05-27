from __future__ import annotations

from pathlib import Path
import importlib
import sys
import types

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from orm_loader.helpers import Base


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SEMANTICS_SRC = ROOT.parent / "omop-semantics" / "src"

for path in (SRC, SEMANTICS_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _load_primary_dx_module(monkeypatch):
    constructs = importlib.import_module("omop_constructs.core.constructs")
    for name in (
        "stage_modifier_mv",
        "modified_conditions_mv",
        "primary_diagnosis_condition_mv",
    ):
        constructs._CONSTRUCTS.pop(name, None)

    modified_source = (
        sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(1).label("person_id"),
            sa.literal(None).label("condition_start_date"),
            sa.literal(1).label("condition_occurrence_id"),
            sa.literal("src").label("condition_source_value"),
            sa.literal(1).label("condition_concept_id"),
            sa.literal("concept").label("condition_concept"),
            sa.literal("code").label("condition_code"),
            sa.literal(10).label("condition_episode"),
            sa.literal(None).label("t_stage_id"),
            sa.literal(None).label("t_stage_date"),
            sa.literal(None).label("t_stage_concept_id"),
            sa.literal(None).label("t_stage_label"),
            sa.literal(None).label("n_stage_id"),
            sa.literal(None).label("n_stage_date"),
            sa.literal(None).label("n_stage_concept_id"),
            sa.literal(None).label("n_stage_label"),
            sa.literal(None).label("m_stage_id"),
            sa.literal(None).label("m_stage_date"),
            sa.literal(None).label("m_stage_concept_id"),
            sa.literal(None).label("m_stage_label"),
            sa.literal(None).label("group_stage_id"),
            sa.literal(None).label("group_stage_date"),
            sa.literal(None).label("group_stage_concept_id"),
            sa.literal(None).label("group_stage_label"),
            sa.literal(None).label("grade_id"),
            sa.literal(None).label("grade_date"),
            sa.literal(None).label("grade_concept_id"),
            sa.literal(None).label("grade_label"),
            sa.literal(None).label("size_id"),
            sa.literal(None).label("size_date"),
            sa.literal(None).label("size_concept_id"),
            sa.literal(None).label("size_label"),
            sa.literal(None).label("laterality_id"),
            sa.literal(None).label("laterality_date"),
            sa.literal(None).label("laterality_concept_id"),
            sa.literal(None).label("laterality_label"),
            sa.literal(None).label("metastatic_disease_id"),
            sa.literal(None).label("metastatic_disease_date"),
            sa.literal(None).label("metastatic_disease_concept_id"),
            sa.literal(None).label("metastatic_disease_label"),
        ).subquery("modified_conditions_source")
    )
    stage_source = (
        sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(1).label("stage_id"),
            sa.literal(1).label("person_id"),
            sa.literal(None).label("condition_start_date"),
            sa.literal(1).label("condition_occurrence_id"),
            sa.literal("src").label("condition_source_value"),
            sa.literal(1).label("condition_concept_id"),
            sa.literal("concept").label("condition_concept"),
            sa.literal("code").label("condition_code"),
            sa.literal(10).label("condition_episode"),
            sa.literal(None).label("stage_date"),
            sa.literal(None).label("stage_concept_id"),
            sa.literal(None).label("stage_label"),
        ).subquery("stage_source")
    )

    condition_join_mod = types.ModuleType(
        "omop_constructs.alchemy.modifiers.condition_modifier_join"
    )
    condition_join_mod.modified_conditions_join = modified_source
    condition_join_mod.all_stage_join = stage_source

    modifier_mappers_mod = types.ModuleType(
        "omop_constructs.alchemy.modifiers.modifier_mappers"
    )
    mv_names = {
        "TStageMV": "t_stage_mv",
        "NStageMV": "n_stage_mv",
        "MStageMV": "m_stage_mv",
        "GroupStageMV": "group_stage_mv",
        "GradeModifierMV": "grade_modifier_mv",
        "SizeModifierMV": "size_modifier_mv",
        "LateralityModifierMV": "laterality_modifier_mv",
        "MetastaticDiseaseModifierMV": "metastatic_disease_modifier_mv",
        "AllStageModifierMV": "all_stage_modifier_mv",
    }
    for name, mv_name in mv_names.items():
        setattr(modifier_mappers_mod, name, type(name, (), {"__mv_name__": mv_name}))

    episode_table = sa.Table(
        "overarching_disease_episode_mv",
        Base.metadata,
        sa.Column("episode_id", sa.Integer, primary_key=True),
        sa.Column("person_id", sa.Integer),
        sa.Column("episode_start_date", sa.Date),
        sa.Column("episode_end_date", sa.Date),
        extend_existing=True,
    )

    class FakeOverarchingDiseaseEpisodeMV:
        __mv_name__ = "overarching_disease_episode_mv"
        __table__ = episode_table
        episode_id = episode_table.c.episode_id
        person_id = episode_table.c.person_id
        episode_start_date = episode_table.c.episode_start_date
        episode_end_date = episode_table.c.episode_end_date

    episodes_mod = types.ModuleType("omop_constructs.alchemy.episodes")
    episodes_mod.OverarchingDiseaseEpisodeMV = FakeOverarchingDiseaseEpisodeMV

    monkeypatch.setitem(
        sys.modules,
        "omop_constructs.alchemy.modifiers.condition_modifier_join",
        condition_join_mod,
    )
    monkeypatch.setitem(
        sys.modules,
        "omop_constructs.alchemy.modifiers.modifier_mappers",
        modifier_mappers_mod,
    )
    monkeypatch.setitem(sys.modules, "omop_constructs.alchemy.episodes", episodes_mod)

    sys.modules.pop("omop_constructs.alchemy.modifiers.condition_modifier_mv", None)
    sys.modules.pop("omop_constructs.alchemy.modifiers", None)

    module = importlib.import_module("omop_constructs.alchemy.modifiers.condition_modifier_mv")
    exports = importlib.import_module("omop_constructs.alchemy.modifiers")
    return module, exports, constructs


def test_primary_diagnosis_condition_mv_is_exported(monkeypatch):
    _, exports, _ = _load_primary_dx_module(monkeypatch)

    assert exports.PrimaryDiagnosisConditionMV.__mv_name__ == "primary_diagnosis_condition_mv"


def test_primary_diagnosis_condition_mv_is_registered(monkeypatch):
    module, _, constructs = _load_primary_dx_module(monkeypatch)
    registry = constructs.get_construct_registry()

    assert registry.get(module.PrimaryDiagnosisConditionMV.__mv_name__) is module.PrimaryDiagnosisConditionMV


def test_primary_diagnosis_condition_mv_compile_check_passes(monkeypatch):
    _, _, constructs = _load_primary_dx_module(monkeypatch)
    registry = constructs.get_construct_registry()

    report = registry.compile_check(require_internal_deps=False)

    assert "modified_conditions_mv" in report
    assert "primary_diagnosis_condition_mv" in report


def test_primary_diagnosis_condition_mv_select_anchors_to_primary_episode(monkeypatch):
    module, _, _ = _load_primary_dx_module(monkeypatch)
    sql = str(
        module.PrimaryDiagnosisConditionMV.__mv_select__.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "modified_conditions_mv" in sql
    assert "overarching_disease_episode_mv" in sql
    assert "primary_diagnosis_episode" in sql
    assert "SELECT DISTINCT" in sql
    assert "episode_start_date" in sql
