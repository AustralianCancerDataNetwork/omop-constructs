"""Pin the semantic anchor sets that the resolver builders depend on.

``DEFAULT_RESOLVER_BUILDERS`` turns these concept lists into vocabulary-backed
resolvers, whose expanded concept sets are inlined into materialized view SQL.
Nothing else in the suite asserts on them: the schema snapshot artifact records
only column metadata, and the registry compile check never diffs generated SQL.
A governed group gaining or losing an anchor would therefore change every
dependent view silently.

These tests need no database — they read the omop-semantics runtime only — so
they run in the default suite rather than behind the ``postgres`` marker.

Expected values are written as literals on purpose. Deriving them from
``runtime`` would make the assertions vacuous.
"""

from omop_semantics.runtime.default_valuesets import runtime


# Group-backed staging units. ``parent_ids`` is the supported accessor; ``ids``
# is deprecated for group-backed units because it means "descendant-expanding
# anchors" without saying so.
EXPECTED_STAGE_ANCHORS = {
    "t": {1634213, 1634376, 1634530, 1634654, 1635114, 1635562, 1635564, 1635682},
    "n": {1633440, 1633885, 1634119, 1634434, 1635320, 1635445},
    "m": {1633547, 1635142, 1635624},
    "group": {1633306, 1633308, 1633650, 1633754, 1634209},
}

# rt_procedure, rt_externalbeam, rt_brachytherapy.
EXPECTED_RADIOTHERAPY_ANCHORS = {1242725, 4141448, 40317890}


def test_stage_anchor_sets_are_stable() -> None:
    for stage, expected in EXPECTED_STAGE_ANCHORS.items():
        unit = getattr(runtime.staging, f"{stage}_stage_concepts")
        assert set(unit.parent_ids) == expected, (
            f"TNM {stage}-stage anchors changed; every dependent modifier view "
            f"will change with them"
        )


def test_radiotherapy_group_matches_governed_anchors() -> None:
    assert set(runtime.cancer_procedures.radiotherapy.parent_ids) == (
        EXPECTED_RADIOTHERAPY_ANCHORS
    )


def test_radiotherapy_group_excludes_non_radiotherapy_procedure_types() -> None:
    """``cancer_procedure_types`` also carries rn_procedure and rt_course, which
    are deliberately not radiotherapy. Guards against the group being widened to
    the whole enumerator."""
    procedure_types = runtime.cancer_procedures.cancer_procedure_types
    governed = set(runtime.cancer_procedures.radiotherapy.parent_ids)
    for label in ("rn_procedure", "rt_course"):
        assert getattr(procedure_types, label) not in governed
