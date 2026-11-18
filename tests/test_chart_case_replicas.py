"""Synthetic replicas of the two chart-reviewed lung 13 cases (INDICATOR_QUERY_SUMMARY.md), plus a true positive.

Dates follow the chart cases; drug patterns are assumptions (SCLC induction then 3- or 4-weekly maintenance).
Episodes:
  1  Early diagnosis shape, maintenance dose ON an RT day       dx 2024-07-19, RT 2025-01-06..01-24, 21-day maintenance
  2  Early diagnosis shape, maintenance spans RT, no dose in it  same dates, 28-day maintenance (doses 2024-12-30, 2025-01-27)
  3  Progression shape: early RT, chemo, progression RT      dx 2024-07-11, RT 2024-08-13..08-22, chemo 2024-09-11..2025-12-10,
     credited to the parent (episode 4 = progression)    progression RT 2025-12-08..12-31 credited to 3
  5  true concurrent cisplatin-RT                          dx 2024-07-01, RT 2024-08-01..09-12, cisplatin 08-01/08-22/09-12
"""
from datetime import date, timedelta

import pytest
import sqlalchemy as sa




def every(start, days, until):
    d = start
    while d <= until:
        yield d
        d += timedelta(days=days)


@pytest.fixture
def chart_inputs(pg_db):
    from omop_alchemy.cdm.model import Episode
    from omop_semantics.runtime.default_valuesets import runtime
    from omop_constructs import get_complete_construct_registry

    get_complete_construct_registry()
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
        ):
            db.exec_driver_sql(f"CREATE TEMP TABLE {definition} ON COMMIT DROP")

        types = runtime.types.disease_episode_types
        dx = {1: date(2024, 7, 19), 2: date(2024, 7, 19), 3: date(2024, 7, 11), 4: date(2025, 11, 17), 5: date(2024, 7, 1)}
        db.exec_driver_sql("ALTER TABLE episode DISABLE TRIGGER ALL")
        for ep, start in dx.items():
            kind = types.disease_progression if ep == 4 else types.episode_of_care
            db.execute(sa.insert(Episode).values(
                episode_id=ep, person_id=1, episode_concept_id=kind, episode_start_date=start,
                episode_object_concept_id=0, episode_type_concept_id=0, episode_parent_id=3 if ep == 4 else None))
            db.exec_driver_sql("INSERT INTO modified_conditions_mv VALUES (1,%s,%s)", (ep, start))
        db.exec_driver_sql("ALTER TABLE episode ENABLE TRIGGER ALL")

        rt = [  # (target episode, course, first, last)
            (1, 101, date(2025, 1, 6), date(2025, 1, 24)),
            (2, 201, date(2025, 1, 6), date(2025, 1, 24)),
            (3, 301, date(2024, 8, 13), date(2024, 8, 22)),
            (4, 302, date(2025, 12, 8), date(2025, 12, 31)), (3, 302, date(2025, 12, 8), date(2025, 12, 31)),
            (5, 501, date(2024, 8, 1), date(2024, 9, 12)),
        ]
        for target, course, first, last in rt:
            db.exec_driver_sql("INSERT INTO rt_course_mv VALUES (1,%s,%s,%s,%s)", (target, course, first, last))

        regimens = [  # (episode, regimen, doses)
            (1, 11, list(every(date(2024, 8, 5), 21, date(2024, 10, 7)))),       # induction
            (1, 12, list(every(date(2024, 11, 4), 21, date(2025, 9, 1)))),       # maintenance incl. 2025-01-06
            (2, 21, list(every(date(2024, 8, 5), 21, date(2024, 10, 7)))),
            (2, 22, list(every(date(2024, 11, 4), 28, date(2025, 9, 1)))),       # 2024-12-30, 2025-01-27: none in RT
            (3, 31, list(every(date(2024, 9, 11), 21, date(2025, 12, 10))) + [date(2025, 12, 10)]),
            (5, 51, [date(2024, 8, 1), date(2024, 8, 22), date(2024, 9, 12)]),
        ]
        cycle = 0
        for ep, regimen, doses in regimens:
            for dose in sorted(set(doses)):
                cycle += 1
                db.exec_driver_sql("INSERT INTO sact_treatment_mv VALUES (1,%s,%s,%s,%s,%s)", (ep, regimen, cycle, dose, dose))
                db.exec_driver_sql("INSERT INTO cycle_mv VALUES (1,%s,%s,%s,%s,%s)", (regimen, cycle, cycle, dose, dose))
        yield db, dx


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_chart_case_replicas(chart_inputs):
    from omop_constructs.alchemy.episodes.treatment_envelope_mv import TreatmentEnvelopeMV

    db, dx = chart_inputs
    env = TreatmentEnvelopeMV.__mv_select__.subquery()
    rows = db.execute(sa.select(env.c.condition_episode, env.c.concurrent_chemort, env.c.first_concurrent_rt_start)).all()
    result = {ep: (flag, start, (start - dx[ep]).days if start else None) for ep, flag, start in rows}
    for ep in sorted(result):
        print(f"episode {ep}: concurrent={result[ep][0]} first_concurrent_rt_start={result[ep][1]} "
              f"days_from_dx={result[ep][2]} -> lung 13 window 0-90: "
              f"{'IN' if result[ep][2] is not None and 0 <= result[ep][2] <= 90 else 'out'}")

    # Flag = "a dose inside an RT course at any point". First-line timing is left to indicator windows,
    # which now work because the flag carries the RT course date.
    assert result[1][:2] == (True, date(2025, 1, 6))     # maintenance dose on an RT day still counts
    assert result[2][:2] == (False, None)                # ongoing regimen without a dose in the course does not
    assert result[3][:2] == (True, date(2025, 12, 8))    # progression RT overlapping last chemo, credited to parent
    assert result[5][:2] == (True, date(2024, 8, 1))     # true concurrent cisplatin-RT
    # lung 13 numerator window 0-90 days from diagnosis: only the true positive survives
    assert [ep for ep in (1, 2, 3, 5) if result[ep][2] is not None and 0 <= result[ep][2] <= 90] == [5]
