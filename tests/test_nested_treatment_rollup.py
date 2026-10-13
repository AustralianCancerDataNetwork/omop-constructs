"""Q3 PostgreSQL source-identity, hierarchy, count and absence regressions."""

from datetime import date, datetime, timedelta

import pytest
import sqlalchemy as sa
from omop_alchemy.cdm import model as cdm
from omop_alchemy.maintenance.tables import collect_maintenance_tables
from omop_constructs import get_complete_construct_registry
from test_construct_registry_postgres import _clear_construct_import_state


START = date(2024, 1, 1)
SURGERY, RT, DRUG, PRESCRIPTION, CONDITION, ROUTE = range(2_100_000_001, 2_100_000_007)
INTENT = 4179711


def _seed_vocabulary(conn):
    identifiers = {0, 32533, 32949, 32944, 32531, 32532, INTENT, SURGERY, RT, DRUG,
                   PRESCRIPTION, CONDITION, ROUTE, 4301351, 1242725, 4161415, 1340204,
                   756290, 1147082, 1147707, 1147127}
    conn.execute(cdm.Concept.__table__.insert(), [dict(
        concept_id=i, concept_name=f"Synthetic {i}", domain_id="Procedure", vocabulary_id="Synthetic",
        concept_class_id="Synthetic", standard_concept="S", concept_code=str(i),
        valid_start_date=START, valid_end_date=date(2099, 1, 1),
    ) for i in identifiers])
    pairs = {(i, i) for i in identifiers} | {(4301351, SURGERY), (4301351, RT), (1242725, RT)}
    conn.execute(cdm.Concept_Ancestor.__table__.insert(), [dict(
        ancestor_concept_id=a, descendant_concept_id=d,
        min_levels_of_separation=int(a != d), max_levels_of_separation=int(a != d),
    ) for a, d in pairs])
    conn.execute(cdm.Person.__table__.insert(), [dict(person_id=i, year_of_birth=1960,
        gender_concept_id=0, race_concept_id=0, ethnicity_concept_id=0) for i in (1, 2)])


def _episode(conn, identifier, concept, parent=None, person=1):
    conn.execute(cdm.Episode.__table__.insert().values(
        episode_id=identifier, episode_concept_id=concept, episode_parent_id=parent, person_id=person,
        episode_start_date=START, episode_end_date=START + timedelta(days=300),
        episode_object_concept_id=CONDITION, episode_type_concept_id=0, episode_number=1))


def _link(conn, owner, event, field):
    conn.execute(cdm.Episode_Event.__table__.insert().values(
        episode_id=owner, event_id=event, episode_event_field_concept_id=field))


def _procedure(conn, identifier, concept, owner, person=1):
    conn.execute(cdm.Procedure_Occurrence.__table__.insert().values(
        procedure_occurrence_id=identifier, procedure_concept_id=concept, person_id=person,
        procedure_date=START + timedelta(days=40),
        procedure_datetime=datetime(2024, 1, 1, 12) + timedelta(days=40), procedure_type_concept_id=0))
    _link(conn, owner, identifier, 1147082)


def _intent(conn, identifier, event, field=756290, person=1):
    conn.execute(cdm.Measurement.__table__.insert().values(
        measurement_id=identifier, person_id=person, measurement_concept_id=INTENT,
        measurement_date=START, measurement_type_concept_id=0, measurement_event_id=event,
        meas_event_field_concept_id=field, measurement_datetime=datetime(2024, 1, 1, 12)))


def _seed_care(conn):
    for identifier, concept, parent, person in (
        (100, 32533, None, 1), (101, 32949, 100, 1), (200, 32533, None, 1),
        (1000, 32531, 101, 1), (2000, 32531, 100, 1), (3000, 32531, 101, 1),
        (1100, 32532, 1000, 1), (1101, 32532, 1000, 1), (2100, 32532, 2000, 1),
        (3100, 32532, 3000, 1), (3101, 32532, 3000, 1), (3102, 32532, 3000, 1),
        (1200, 32532, 1000, 2), (4000, 32531, 101, 2), (4100, 32532, 4000, 2),
    ):
        _episode(conn, identifier, concept, parent, person)
    for identifier, owner, offset in ((400, 100, 0), (401, 100, 5), (402, 101, 30), (403, 200, 0)):
        conn.execute(cdm.Condition_Occurrence.__table__.insert().values(
            condition_occurrence_id=identifier, condition_concept_id=CONDITION, person_id=1,
            condition_start_date=START + timedelta(days=offset), condition_type_concept_id=0))
        _link(conn, owner, identifier, 1147127)
    for identifier, cycle, person in ((5001, 1100, 1), (5002, 1101, 1), (5003, 2100, 1),
                                     (5004, 1100, 2), (5005, 1200, 2), (5006, 4100, 2)):
        conn.execute(cdm.Drug_Exposure.__table__.insert().values(
            drug_exposure_id=identifier, person_id=person, drug_concept_id=DRUG, route_concept_id=ROUTE,
            drug_exposure_start_date=START + timedelta(days=40), drug_exposure_end_date=START + timedelta(days=41),
            drug_type_concept_id=0))
        _link(conn, cycle, identifier, 1147707)
    _link(conn, 1100, 5001, 1147707)  # deliberate duplicate in fixture-only relaxed table
    for procedure, owner in ((7001, 1000), (7002, 1000), (7003, 2000), (7004, 3000)):
        _procedure(conn, procedure, PRESCRIPTION, owner)
    _procedure(conn, 7005, PRESCRIPTION, 1000, person=2)
    for measurement, procedure in ((9001, 7001), (9002, 7001), (9003, 7002), (9004, 7003), (9005, 7004)):
        _intent(conn, measurement, procedure, 1147082)
    for procedure, fraction in ((6101, 3100), (6102, 3101), (6103, 3102)):
        _procedure(conn, procedure, RT, fraction)
    for identifier in (9201, 9202):
        _intent(conn, identifier, 6101, 1147082)
    for identifier, episode in ((9301, 1100), (9302, 1100), (9401, 1000)):
        _intent(conn, identifier, episode)
    _intent(conn, 9901, 7001, 1147082, person=2)
    _intent(conn, 9902, 1100, person=2)
    _intent(conn, 9903, 6102, 1147082, person=2)  # foreign-only intent must retain the fraction
    _procedure(conn, 8001, SURGERY, 101)
    _link(conn, 100, 8001, 1147082)
    _link(conn, 101, 8001, 1147082)
    _procedure(conn, 5001, SURGERY, 1100)  # same number as a drug, different source table
    _link(conn, 101, 5001, 1147707)  # cannot supply surgery evidence
    conn.execute(cdm.Observation.__table__.insert().values(
        observation_id=8002, person_id=1, observation_concept_id=1340204, value_as_concept_id=SURGERY,
        observation_date=START, observation_type_concept_id=0))


@pytest.fixture
def treatment_registry(pg_db):
    engine = pg_db.engine
    schema = pg_db.resolved.cdm_schema
    with engine.begin() as conn:
        # Real mapped CDM columns; corruption relaxation is confined to this disposable test DB.
        for source in collect_maintenance_tables():
            for constraint in source.table.constraints:
                if isinstance(constraint, sa.ForeignKeyConstraint):
                    conn.execute(sa.schema.DropConstraint(constraint, if_exists=True))
        conn.execute(sa.schema.DropConstraint(cdm.Episode_Event.__table__.primary_key, if_exists=True))
        _seed_vocabulary(conn)
        _seed_care(conn)
    _clear_construct_import_state()
    registry = get_complete_construct_registry()
    try:
        with engine.begin() as conn:
            registry.create_missing(conn, schema=schema)
        yield engine, registry
    finally:
        with engine.begin() as conn:
            registry.drop_all(conn, cascade=True)


def _rows(fixture, sql):
    with fixture[0].connect() as conn:
        return conn.execute(sa.text(sql)).all()


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_child_rollup_keeps_direct_credit_and_isolates_second_primary(treatment_registry):
    assert _rows(treatment_registry, "SELECT dx_episode_id,treatment_regimen_count FROM dx_treat_start_mv ORDER BY 1") == [(100, 3), (101, 2)]
    assert _rows(treatment_registry, "SELECT DISTINCT condition_episode_id,regimen_id FROM sact_treatment_mv ORDER BY 1,2") == [(100, 1000), (100, 2000), (101, 1000)]


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_activity_counts_do_not_follow_prescription_or_intent_multiplicity(treatment_registry):
    assert _rows(treatment_registry, "SELECT count(*) FROM sact_treatment_mv WHERE regimen_id=1000") == [(12,)]
    assert _rows(treatment_registry, "SELECT DISTINCT exposure_count FROM sact_treatment_mv") == [(1,)]
    assert _rows(treatment_registry, "SELECT DISTINCT regimen_id,exposure_count,regimen_count FROM condition_treatment_episode_mv WHERE regimen_id IS NOT NULL ORDER BY 1") == [(1000, 2, 1), (2000, 1, 1)]
    assert _rows(treatment_registry, "SELECT DISTINCT fraction_count,course_count FROM condition_treatment_episode_mv WHERE course_id IS NOT NULL") == [(3, 1)]


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_performed_surgery_deduplicates_root_child_links_and_keeps_null_spine(treatment_registry):
    assert _rows(treatment_registry, "SELECT condition_episode_id,surgery_occurrence_id FROM surgical_procedure_mv ORDER BY 1") == [(100, 8001), (101, 8001), (200, None)]


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_intent_source_identity_survives_same_concept_and_envelope_date_spine(treatment_registry):
    assert _rows(treatment_registry, "SELECT intent_measurement_id FROM condition_episode_intent_mv WHERE episode_id=100 ORDER BY 1") == [(9301,), (9302,), (9401,)]
    assert _rows(treatment_registry, "SELECT count(*) FROM treatment_envelope_mv WHERE condition_episode=100") == [(2,)]
    assert _rows(treatment_registry, "SELECT DISTINCT treatment_episode_parent_id,sact,rt FROM episode_treatment_mv WHERE intent_measurement_id=9401") == [(101, None, None)]
    assert _rows(treatment_registry, "SELECT intent_id FROM fraction_mv WHERE procedure_occurrence_id=6101 ORDER BY 1") == [(9201,), (9202,)]


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_invalid_source_person_and_discriminator_cannot_supply_care(treatment_registry):
    assert _rows(treatment_registry, "SELECT drug_exposure_id FROM cycle_mv ORDER BY 1") == [(5001,), (5002,), (5003,)]
    assert _rows(treatment_registry, "SELECT DISTINCT intent_id FROM sact_treatment_mv WHERE regimen_id=1000 ORDER BY 1") == [(9001,), (9002,), (9003,)]
    assert _rows(treatment_registry, "SELECT intent_id FROM fraction_mv WHERE procedure_occurrence_id=6102") == [(None,)]


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_added_columns_work_through_mapped_classes_and_keep_rt_timestamp(treatment_registry):
    engine, registry = treatment_registry
    intent = registry.get("condition_episode_intent_mv")
    with engine.connect() as conn:
        assert conn.execute(sa.select(intent.intent_measurement_id).where(intent.episode_id == 100).order_by(intent.intent_measurement_id)).scalars().all() == [9301, 9302, 9401]
    assert _rows(treatment_registry, "SELECT DISTINCT pg_typeof(course_start_date)::text FROM condition_treatment_episode_mv WHERE course_id IS NOT NULL") == [("timestamp without time zone",)]
