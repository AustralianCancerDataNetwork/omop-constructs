from __future__ import annotations

import importlib
import sys
from datetime import date, timedelta

import pytest
import sqlalchemy as sa
from omop_constructs.core.orm_base import ConstructORMBase as Base
from orm_loader.helpers.bootstrap import create_db

from omop_constructs import get_complete_construct_registry
from omop_constructs.core.registry import materialized_view_exists


def _clear_construct_import_state() -> None:
    constructs = importlib.import_module("omop_constructs.core.constructs")
    registered = list(constructs._CONSTRUCTS.values())
    constructs._CONSTRUCTS.clear()

    for cls in registered:
        table = getattr(cls, "__table__", None)
        if table is not None and table.key in Base.metadata.tables:
            Base.metadata.remove(Base.metadata.tables[table.key])

    for module_name in list(sys.modules):
        if (
            module_name.startswith(
                ("omop_constructs.alchemy", "omop_constructs.semantics")
            )
            or module_name in {"omop_constructs.bootstrap", "omop_constructs.bootstrap_modules"}
        ):
            sys.modules.pop(module_name, None)


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_full_construct_registry_compile_check_on_postgres(pg_db):
    _clear_construct_import_state()
    registry = get_complete_construct_registry()

    report = registry.compile_check()
    assert "primary_diagnosis_condition_mv" in report

    with pg_db.engine.connect() as conn:
        created = []
        try:
            created = registry.create_missing(
                conn, with_data=False, schema=pg_db.resolved.cdm_schema
            )
            assert "primary_diagnosis_condition_mv" in created
            assert materialized_view_exists(
                conn,
                "primary_diagnosis_condition_mv",
                schema=pg_db.resolved.cdm_schema,
            )
        finally:
            registry.drop_all(conn, cascade=True)


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_orm_loader_can_bootstrap_after_construct_import(pg_db):
    get_complete_construct_registry()
    create_db(pg_db.resolved, bindable=pg_db.engine)


@pytest.fixture
def crt_inputs(pg_db):
    """Supply synthetic course, dose and intent rows at their view boundaries."""
    from omop_alchemy.cdm.model import Episode
    from omop_semantics.runtime.default_valuesets import runtime

    get_complete_construct_registry()
    start = date(2026, 1, 1)

    with pg_db.engine.begin() as db:
        for definition in (
            "modified_conditions_mv (person_id int, condition_episode int, condition_start_date date)",
            "surgical_procedure_mv (person_id int, condition_episode_id int, surgery_datetime timestamp)",
            "sact_treatment_mv (person_id int, condition_episode_id int, regimen_id int, cycle_id int, "
            "first_exposure_date date, last_exposure_date date)",
            "rt_course_mv (person_id int, condition_episode_id int, course_id int, "
            "first_exposure_date timestamp, last_exposure_date timestamp)",
            "cycle_mv (person_id int, regimen_id int, cycle_id int, drug_exposure_id int, "
            "drug_exposure_start_date date, drug_exposure_end_date date)",
            "condition_episode_mv (episode_id int, person_id int, episode_concept_id int, episode_label text, "
            "episode_start_date date, episode_end_date date)",
            "episode_treatment_mv (person_id int, intent_measurement_id int, treatment_episode_id int, "
            "treatment_episode_start_date date, treatment_episode_end_date date, "
            "treatment_episode_parent_id int, treatment_intent_concept_id int, treatment_intent_name text, "
            "rt bool, sact bool, concurrent bool)",
        ):
            db.exec_driver_sql(f"CREATE TEMP TABLE {definition} ON COMMIT DROP")

        disease_types = runtime.types.disease_episode_types
        # The disposable schema has no person or vocabulary records.
        db.exec_driver_sql("ALTER TABLE episode DISABLE TRIGGER ALL")
        for episode in range(1, 10):
            kind = disease_types.disease_progression if episode == 9 else disease_types.episode_of_care
            db.execute(sa.insert(Episode).values(
                episode_id=episode, person_id=1, episode_concept_id=kind,
                episode_start_date=start, episode_object_concept_id=0, episode_type_concept_id=0,
                episode_parent_id=2 if episode == 9 else None,
            ))
            db.exec_driver_sql("INSERT INTO modified_conditions_mv VALUES (1,%s,%s)", (episode, start))
            db.exec_driver_sql("INSERT INTO condition_episode_mv VALUES (%s,1,%s,'Disease',%s,NULL)",
                               (episode, kind, start))
        db.exec_driver_sql("ALTER TABLE episode ENABLE TRIGGER ALL")

        for target, owner, course, first, last in (
            (1, 1, 101, 20, 25),  # Between two regimens.
            (2, 2, 201, 10, 15),  # Early RT, then SACT, then progression RT.
            (2, 9, 202, 80, 85), (9, 9, 202, 80, 85),
            (3, 3, 301, 20, 25),  # Same day.
            (4, 4, 401, 10, 15), (4, 4, 402, 60, 65),  # Only the second course qualifies.
            (5, 5, 501, 20, 25),  # Last day is inclusive.
            (6, 6, 601, 20, 25),  # Dose just before or just after the course.
            (7, 7, 701, 20, 25),  # Dose in another episode or person.
            (8, 8, 801, 20, 25),  # Long cycle has no start in RT, even though its span overlaps.
        ):
            first_day, last_day = start + timedelta(days=first), start + timedelta(days=last)
            db.exec_driver_sql("INSERT INTO rt_course_mv VALUES (1,%s,%s,%s,%s)",
                               (target, course, first_day, last_day))
            if target == owner:
                db.exec_driver_sql("INSERT INTO episode_treatment_mv VALUES "
                                   "(1,%s,%s,%s,%s,%s,1,'Intent',true,false,false)",
                                   (course, course, first_day, last_day, owner))

        for episode, regimen, cycle, first, last, days in (
            (1, 11, 111, 5, 8, (5, 8)), (1, 12, 112, 30, 31, (30,)),
            (2, 21, 211, 30, 100, (30, 100)),
            (3, 31, 311, 20, 20, (20,)),
            (4, 41, 411, 39, 39, (39,)), (4, 41, 412, 60, 62, (60, 62)),
            (5, 51, 511, 25, 25, (25,)),
            (6, 61, 611, 19, 26, (19, 26)),
            (7, 71, 711, 1, 1, (1,)),
            (8, 81, 811, 19, 26, (19, 26)),
        ):
            db.exec_driver_sql("INSERT INTO sact_treatment_mv VALUES (1,%s,%s,%s,%s,%s)",
                               (episode, regimen, cycle, start + timedelta(days=first), start + timedelta(days=last)))
            for day in days:
                dose_day = start + timedelta(days=day)
                db.exec_driver_sql("INSERT INTO cycle_mv VALUES (1,%s,%s,%s,%s,%s)",
                                   (regimen, cycle, cycle * 100 + day, dose_day, dose_day))

        db.exec_driver_sql("INSERT INTO cycle_mv VALUES (2,71,711,1,%s,%s)", (start + timedelta(days=20),) * 2)
        db.exec_driver_sql("INSERT INTO episode_treatment_mv VALUES "
                           "(1,41,41,%s,NULL,4,1,'Intent',false,true,false)",
                           (start + timedelta(days=39),))
        yield db, start


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_concurrent_chemort_requires_a_recorded_dose_during_an_individual_course(crt_inputs):
    from omop_constructs.alchemy.episodes.treatment_envelope_mv import TreatmentEnvelopeMV

    db, start = crt_inputs
    envelope = TreatmentEnvelopeMV.__mv_select__.subquery()
    flags = dict(db.execute(sa.select(envelope.c.condition_episode, envelope.c.concurrent_chemort)).all())
    assert {episode for episode, concurrent in flags.items() if concurrent} == {3, 4, 5}
    assert flags[1] is False  # An RT course in a gap between two regimens.
    assert flags[2] is False  # The same result with nested RT credited to the root.
    assert flags[9] is False  # No qualifying SACT on the progression itself.

    dates = dict(db.execute(sa.select(envelope.c.condition_episode, envelope.c.first_concurrent_rt_start)).all())
    assert {episode: day for episode, day in dates.items() if day is not None} == {
        3: start + timedelta(days=20), 4: start + timedelta(days=60), 5: start + timedelta(days=20),
    }


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_only_the_matching_rt_course_is_concurrent_in_intent(crt_inputs):
    from omop_constructs.alchemy.episodes.modality_intent_mv import ConditionTreatmentIntentMV

    db, _ = crt_inputs
    intent = ConditionTreatmentIntentMV.__mv_select__.subquery()
    rows = db.execute(sa.select(intent.c.treatment_episode_id, intent.c.concurrent, intent.c.concurrent_rt)
                      .where(intent.c.episode_id == 4).order_by(intent.c.treatment_episode_id)).all()
    assert rows == [(41, True, False), (401, False, False), (402, True, True)]
