"""Exercise the release validation SQL against synthetic views with known answers.

The transitional scripts in ``scripts/release_validation/`` are what produce the
OC-0 clinical evidence, so the SQL they generate needs to be correct before an
operator runs it in a secure environment where the output cannot be checked
against a known truth.

Each test builds small materialized views with hand-computed expected numbers.
That is the only way to distinguish "the comparison found no differences" from
"the comparison is not looking at anything" — a failure mode this suite already
hit once, when column discovery silently returned nothing because materialized
views are absent from ``information_schema.columns``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = REPO_ROOT / "scripts" / "release_validation"
for import_root in (REPO_ROOT, SCRIPTS):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from scripts.release_validation import _common  # noqa: E402
from scripts.release_validation.build_side_schema import (  # noqa: E402
    build_driver_environment,
)
from scripts.release_validation.collect_key_metrics import (  # noqa: E402
    measure_construct,
)
from scripts.release_validation.compare_schemas import (  # noqa: E402
    compare_construct,
    except_all_sql,
    person_summary_sql,
)

BASELINE_SCHEMA = "rv_baseline"
CANDIDATE_SCHEMA = "rv_candidate"


def _create_matview(conn: sa.Connection, schema: str, name: str, values_sql: str) -> None:
    conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    conn.execute(
        sa.text(f'CREATE MATERIALIZED VIEW "{schema}"."{name}" AS {values_sql}')
    )


@pytest.fixture
def synthetic_schemas(pg_bootstrapped_engine):
    """Two side schemas holding synthetic views, on the disposable scratch database."""
    engine = pg_bootstrapped_engine
    with engine.begin() as conn:
        for schema in (BASELINE_SCHEMA, CANDIDATE_SCHEMA):
            conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    yield engine


@pytest.mark.postgres
def test_matview_columns_finds_materialized_view_columns(synthetic_schemas):
    """Materialized views are absent from information_schema.columns.

    This is the regression guard for the defect that made every construct report
    "no comparable columns": the standard information schema does not know about
    materialized views, so column discovery has to read pg_attribute.
    """
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "sample_mv",
            "SELECT 1 AS mv_id, 10 AS person_id, 100 AS episode_id",
        )

    assert _common.matview_columns(engine, BASELINE_SCHEMA, "sample_mv") == (
        "mv_id",
        "person_id",
        "episode_id",
    )

    with engine.connect() as conn:
        legacy = conn.execute(
            sa.text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = 'sample_mv'"
            ),
            {"schema": BASELINE_SCHEMA},
        ).scalar_one()
    assert legacy == 0, (
        "information_schema.columns now reports materialized views; the "
        "pg_attribute query is still correct but this note is stale."
    )


@pytest.mark.postgres
def test_measure_construct_counts_duplicates_and_null_keys(synthetic_schemas):
    """Hand-computed key metrics over a view with deliberate duplicates and NULLs.

    Rows, by (person_id, episode_id):
        (1, 100)      x2  -> one duplicate
        (1, NULL)     x2  -> one duplicate, both counted as null-key rows
        (NULL, NULL)  x1  -> one null-key row, distinct from the above
        (2, 300)      x1
    So 6 rows, 3 with a NULL somewhere in the key, 4 distinct keys, 2 duplicates.

    The NULL cases are the point: count(distinct (a, b)) treats NULLs as equal,
    which is stricter than a PostgreSQL unique index and is what stops a
    null-spine construct from hiding duplicate spine rows.
    """
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "dupes_mv",
            """
            SELECT * FROM (VALUES
                (1,    100),
                (1,    100),
                (1,    NULL),
                (1,    NULL),
                (NULL, NULL),
                (2,    300)
            ) AS t(person_id, episode_id)
            """,
        )

    with engine.connect() as conn:
        checksum, row_count, null_key_rows, distinct_keys = measure_construct(
            conn,
            schema=BASELINE_SCHEMA,
            name="dupes_mv",
            logical_key=["person_id", "episode_id"],
        )

    assert row_count == 6
    assert null_key_rows == 3
    assert distinct_keys == 4
    assert row_count - distinct_keys == 2
    assert len(checksum) == 32, "pg_get_viewdef checksum should be an md5 hex digest"


@pytest.mark.postgres
def test_except_all_detects_a_multiplicity_change(synthetic_schemas):
    """A row that appears twice instead of once must be reported as a difference.

    This is the OC-H1 and OC-B2 failure mode: the same distinct rows, a different
    number of copies. Plain EXCEPT would deduplicate and report no difference at
    all, which is why the comparison uses EXCEPT ALL.
    """
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "multi_mv",
            "SELECT * FROM (VALUES (1, 100), (2, 200)) AS t(person_id, episode_id)",
        )
        # Same distinct rows; person 1's row is duplicated.
        _create_matview(
            conn,
            CANDIDATE_SCHEMA,
            "multi_mv",
            "SELECT * FROM (VALUES (1, 100), (1, 100), (2, 200)) AS t(person_id, episode_id)",
        )

    columns = ["person_id", "episode_id"]
    with engine.connect() as conn:
        removed = conn.execute(
            sa.text(
                except_all_sql(
                    left_schema=BASELINE_SCHEMA,
                    right_schema=CANDIDATE_SCHEMA,
                    name="multi_mv",
                    columns=columns,
                )
            )
        ).all()
        added = conn.execute(
            sa.text(
                except_all_sql(
                    left_schema=CANDIDATE_SCHEMA,
                    right_schema=BASELINE_SCHEMA,
                    name="multi_mv",
                    columns=columns,
                )
            )
        ).all()

        deduplicating = conn.execute(
            sa.text(
                f'SELECT person_id, episode_id FROM "{CANDIDATE_SCHEMA}".multi_mv '
                f'EXCEPT '
                f'SELECT person_id, episode_id FROM "{BASELINE_SCHEMA}".multi_mv'
            )
        ).all()

    assert removed == [], "nothing was removed; only a copy was added"
    assert added == [(1, 100)], "the extra copy of person 1's row must surface"
    assert deduplicating == [], (
        "plain EXCEPT misses the multiplicity change, which is exactly why the "
        "comparison uses EXCEPT ALL"
    )


@pytest.mark.postgres
def test_comparison_counts_exactly_but_only_returns_bounded_rows(synthetic_schemas):
    """Metadata stays exact without loading an unbounded clinical result set."""
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "bounded_mv",
            "SELECT n AS person_id, n AS episode_id FROM generate_series(1, 10) n",
        )
        _create_matview(
            conn,
            CANDIDATE_SCHEMA,
            "bounded_mv",
            "SELECT n AS person_id, n AS episode_id FROM generate_series(101, 110) n",
        )

    arguments = {
        "name": "bounded_mv",
        "baseline_schema": BASELINE_SCHEMA,
        "candidate_schema": CANDIDATE_SCHEMA,
        "baseline_columns": ["person_id", "episode_id"],
        "candidate_columns": ["person_id", "episode_id"],
        "max_rows": 2,
    }
    with engine.connect() as conn:
        metadata, _, removed, added, persons = compare_construct(conn, **arguments)

        assert metadata.rows_only_in_baseline == 10
        assert metadata.rows_only_in_candidate == 10
        assert metadata.persons_affected == 20
        assert removed == added == persons == []

        clinical, _, removed, added, persons = compare_construct(
            conn, **arguments, include_rows=True
        )

    assert clinical.rows_only_in_baseline == 10
    assert clinical.rows_only_in_candidate == 10
    assert clinical.persons_affected == 20
    assert len(removed) == len(added) == len(persons) == 2


@pytest.mark.postgres
def test_column_change_is_not_reported_as_row_identity(synthetic_schemas):
    """A shared-column match cannot conceal a changed output contract."""
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "shape_mv",
            "SELECT 1 AS person_id, 10 AS episode_id",
        )
        _create_matview(
            conn,
            CANDIDATE_SCHEMA,
            "shape_mv",
            "SELECT 1 AS person_id, 10 AS episode_id, 'new'::text AS status",
        )

    with engine.connect() as conn:
        diff, _, removed, added, persons = compare_construct(
            conn,
            name="shape_mv",
            baseline_schema=BASELINE_SCHEMA,
            candidate_schema=CANDIDATE_SCHEMA,
            baseline_columns=["person_id", "episode_id"],
            candidate_columns=["person_id", "episode_id", "status"],
            include_rows=True,
            max_rows=2,
        )

    assert diff.comparable is False
    assert diff.identical is None
    assert diff.candidate_only_columns == "status"
    assert "shared subset" in diff.note
    assert removed == added == persons == []


@pytest.mark.postgres
def test_person_summary_attributes_deltas_to_the_right_people(synthetic_schemas):
    """Per-person added/removed counts over a known delta.

    Baseline: person 1 has two rows, person 2 has one, person 3 has one.
    Candidate: person 1 loses a row, person 2 is unchanged, person 3's row
    changed episode, and person 4 is new.

    Expected: person 1 net -1, person 3 net 0 but one row each way, person 4 net
    +1, and person 2 absent entirely.
    """
    engine = synthetic_schemas
    with engine.begin() as conn:
        _create_matview(
            conn,
            BASELINE_SCHEMA,
            "people_mv",
            """
            SELECT * FROM (VALUES
                (1, 100), (1, 101), (2, 200), (3, 300)
            ) AS t(person_id, episode_id)
            """,
        )
        _create_matview(
            conn,
            CANDIDATE_SCHEMA,
            "people_mv",
            """
            SELECT * FROM (VALUES
                (1, 100), (2, 200), (3, 301), (4, 400)
            ) AS t(person_id, episode_id)
            """,
        )

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                person_summary_sql(
                    baseline_schema=BASELINE_SCHEMA,
                    candidate_schema=CANDIDATE_SCHEMA,
                    name="people_mv",
                    columns=["person_id", "episode_id"],
                )
            )
        ).mappings().all()

    by_person = {row["person_id"]: row for row in rows}

    assert 2 not in by_person, "person 2 is unchanged and must not appear"

    assert by_person[1]["removed_rows"] == 1
    assert by_person[1]["added_rows"] == 0
    assert by_person[1]["net_rows"] == -1

    assert by_person[3]["removed_rows"] == 1
    assert by_person[3]["added_rows"] == 1
    assert by_person[3]["net_rows"] == 0, (
        "a changed row must show on both sides rather than netting out of the report"
    )

    assert by_person[4]["removed_rows"] == 0
    assert by_person[4]["added_rows"] == 1
    assert by_person[4]["net_rows"] == 1


@pytest.mark.postgres
def test_clinical_output_is_refused_inside_the_repository(tmp_path):
    """The path guard is the last line of defence against committing patient data."""
    repo_root = Path(__file__).resolve().parents[1]

    with pytest.raises(_common.ValidationError, match="inside the repository"):
        _common.prepare_output_dir(repo_root / "scratch_run", clinical=True)

    # Non-clinical metadata may be written anywhere, including the repository.
    paths = _common.prepare_output_dir(tmp_path / "run", clinical=False)
    assert paths.metadata.is_dir()
    assert not paths.clinical.exists()


def test_side_build_environment_forces_imports_onto_the_explicit_cdm(
    tmp_path, monkeypatch
):
    """Import-time resolvers and the DDL engine must use the same database."""
    interpreter = tmp_path / "side" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    monkeypatch.setenv("OA_CONFIG_PATH", "/operator/stack.toml")
    monkeypatch.setenv("ENGINE_CDM", "postgresql://wrong")

    expected = "postgresql://release-cdm"
    environment = build_driver_environment(interpreter, expected)

    assert environment["ENGINE_CDM"] == expected
    assert environment["ENGINE"] == expected
    assert environment["OA_CONFIG_PATH"] == str(
        interpreter.parent / ".oc0-explicit-cdm-fallback.toml"
    )
    assert "invalid = [" in Path(environment["OA_CONFIG_PATH"]).read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize(
    "unsafe", ["public; drop schema public", 'bad"name', "MixedCase", "has-dash"]
)
def test_release_schema_identifiers_reject_sql_fragments(unsafe):
    with pytest.raises(_common.ValidationError, match="plain lower-case"):
        _common.quote_identifier(unsafe)


# ---------------------------------------------------------------------------
# SQL checksum normalisation
# ---------------------------------------------------------------------------

def test_normalisation_sorts_numeric_in_lists():
    """Reordering a concept-ID list must not change the checksum.

    Construct queries embed resolved concept sets as literal IN lists rendered in
    Python set-iteration order, which depends on PYTHONHASHSEED. Without sorting,
    the same construct checksums differently on every run and the
    "which definitions changed?" signal is worthless.
    """
    from scripts.release_validation._sql_normalise import (
        normalise_sql,
        raw_checksum,
        sql_checksum,
    )

    a = "SELECT x FROM t WHERE concept_id IN (3, 1, 2)"
    b = "SELECT x FROM t WHERE concept_id IN (1, 2, 3)"

    assert normalise_sql(a) == normalise_sql(b)
    assert sql_checksum(a) == sql_checksum(b)
    assert raw_checksum(a) != raw_checksum(b), (
        "the raw checksum must stay order-sensitive so ordering-only differences "
        "remain visible"
    )


def test_normalisation_still_detects_a_changed_concept_set():
    """Sorting must not blunt the check it exists to make reliable."""
    from scripts.release_validation._sql_normalise import sql_checksum

    three = "SELECT x FROM t WHERE concept_id IN (1, 2, 3)"
    four = "SELECT x FROM t WHERE concept_id IN (1, 2, 3, 4)"
    swapped = "SELECT x FROM t WHERE concept_id IN (1, 2, 9)"

    assert sql_checksum(three) != sql_checksum(four)
    assert sql_checksum(three) != sql_checksum(swapped)


def test_normalisation_leaves_non_numeric_in_lists_alone():
    """Only all-numeric lists are sorted.

    Reordering a concept-ID list preserves meaning. Reordering something the
    pattern does not recognise might not, so those are left exactly as rendered
    rather than silently rewritten.
    """
    from scripts.release_validation._sql_normalise import normalise_sql

    for sql in (
        "SELECT x FROM t WHERE code IN ('b', 'a')",
        "SELECT x FROM t WHERE id IN (SELECT id FROM other ORDER BY seq)",
        "SELECT x FROM t WHERE (a, b) IN ((2, 1), (1, 2))",
    ):
        assert normalise_sql(sql) == sql


def test_normalisation_handles_several_lists_and_odd_spacing():
    from scripts.release_validation._sql_normalise import normalise_sql

    normalised = normalise_sql(
        "SELECT x FROM t WHERE a in(  3,1 , 2 ) AND b IN (9,8) AND c = 5"
    )
    assert "IN (1, 2, 3)" in normalised
    assert "IN (8, 9)" in normalised
    assert "c = 5" in normalised


@pytest.mark.postgres
def test_real_construct_checksums_are_stable_across_hash_seeds(pg_bootstrapped_engine):
    """The normalised checksum of a real construct must not depend on hash seed.

    Uses constructs whose queries embed a resolved concept set, since those are
    the ones affected, and asserts the rendered SQL really does contain a numeric
    IN list — otherwise the test would pass while exercising nothing.

    Whether the *raw* checksum differs for a given pair of seeds is luck: a small
    concept set often lands in the same order. The order-sensitivity of the raw
    checksum is asserted deterministically on synthetic SQL in
    ``test_normalisation_sorts_numeric_in_lists`` instead.
    """
    import json
    import os
    import subprocess
    import sys

    targets = ("t_stage_mv", "fraction_mv", "dtherm_dx_mv")

    program = (
        "import json, re, sys\n"
        f"sys.path.insert(0, {str(SCRIPTS)!r})\n"
        "from _sql_normalise import sql_checksum\n"
        "from sqlalchemy.dialects import postgresql\n"
        "from omop_constructs.bootstrap import get_complete_construct_registry\n"
        "r = get_complete_construct_registry()\n"
        "out = {}\n"
        f"for name in {targets!r}:\n"
        "    sql = str(r.get(name).__mv_select__.compile("
        "dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))\n"
        "    has_numeric_in = bool(re.search(r'\\bIN\\s*\\(\\s*\\d+', sql, re.IGNORECASE))\n"
        "    out[name] = [sql_checksum(sql), has_numeric_in]\n"
        "print(json.dumps(out))\n"
    )

    runs = []
    for seed in ("1", "2", "12345"):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        result = subprocess.run(
            [sys.executable, "-c", program], capture_output=True, text=True, env=env
        )
        assert result.returncode == 0, result.stderr
        runs.append(json.loads(result.stdout.strip().splitlines()[-1]))

    first = runs[0]

    assert any(first[name][1] for name in targets), (
        "none of the sampled constructs renders a numeric IN list, so this test "
        "is not exercising the ordering instability it exists to guard"
    )

    for name in targets:
        checksums = {run[name][0] for run in runs}
        assert len(checksums) == 1, (
            f"{name}: normalised checksum varies with PYTHONHASHSEED "
            f"({checksums}), so definition comparison would report false changes"
        )


def test_schema_qualification_is_stripped_for_the_measured_schema_only():
    """Deployed checksums must be comparable between the two side schemas.

    PostgreSQL stores the qualified name of every referenced object, so a
    construct built into ``oc_baseline`` names that schema inside its own
    definition. Comparing raw definitions between sides would therefore report
    every dependent construct as changed.
    """
    from scripts.release_validation._sql_normalise import (
        sql_checksum,
        strip_schema_qualification,
    )

    baseline = (
        'SELECT a FROM oc_baseline.condition_episode_mv '
        'JOIN public.person ON true'
    )
    candidate = baseline.replace("oc_baseline", "oc_candidate")

    assert sql_checksum(baseline, strip_schema="oc_baseline") == sql_checksum(
        candidate, strip_schema="oc_candidate"
    )

    # The CDM schema stays qualified, so a candidate reading a different CDM
    # schema is still a detected change.
    stripped = strip_schema_qualification(baseline, "oc_baseline")
    assert "public.person" in stripped
    assert "condition_episode_mv" in stripped
    assert "oc_baseline" not in stripped

    moved_cdm = candidate.replace("public.person", "other_cdm.person")
    assert sql_checksum(candidate, strip_schema="oc_candidate") != sql_checksum(
        moved_cdm, strip_schema="oc_candidate"
    )


def test_schema_stripping_does_not_match_a_similarly_named_schema():
    """`oc_base` must not be stripped out of `oc_baseline`."""
    from scripts.release_validation._sql_normalise import strip_schema_qualification

    sql = "SELECT a FROM oc_baseline.t JOIN oc_base.u ON true"
    stripped = strip_schema_qualification(sql, "oc_base")

    assert "oc_baseline.t" in stripped, "the longer schema name must survive"
    assert "JOIN u ON true" in stripped


def test_normalisation_handles_the_postgres_any_array_rendering():
    """`pg_get_viewdef` rewrites IN lists, so both forms must be normalised.

    PostgreSQL renders `x IN (1, 2, 3)` back as `x = ANY (ARRAY[1, 2, 3])`.
    Normalising only the SQLAlchemy form would leave every deployed definition
    unstable — and the deployed definition is what the release comparison reads.
    """
    from scripts.release_validation._sql_normalise import normalise_sql, sql_checksum

    unsorted = "WHERE concept_id = ANY (ARRAY[3, 1, 2])"
    sorted_ = "WHERE concept_id = ANY (ARRAY[1, 2, 3])"

    assert normalise_sql(unsorted) == normalise_sql(sorted_)
    assert sql_checksum(unsorted) == sql_checksum(sorted_)
    assert sql_checksum(unsorted) != sql_checksum("WHERE concept_id = ANY (ARRAY[1, 2, 4])")


def test_normalisation_preserves_an_any_array_type_cast():
    """PostgreSQL appends a cast for non-integer element types; keep it."""
    from scripts.release_validation._sql_normalise import normalise_sql

    normalised = normalise_sql("WHERE id = ANY (ARRAY[2, 1]::bigint[])")
    assert normalised == "WHERE id = ANY (ARRAY[1, 2]::bigint[])"


def test_the_two_renderings_of_one_list_agree_after_normalisation():
    """A compiled select and its deployed definition must checksum alike.

    This is what lets the compiled-checksum verdict from the build step and the
    deployed-checksum comparison from the metrics step cross-check each other.
    """
    from scripts.release_validation._sql_normalise import sql_checksum

    compiled = "SELECT x FROM t WHERE concept_id IN (3, 1, 2)"
    deployed = "SELECT x FROM t WHERE concept_id IN (1, 2, 3)"
    assert sql_checksum(compiled) == sql_checksum(deployed)


@pytest.mark.postgres
def test_unpopulated_views_are_reported_not_raised(synthetic_schemas):
    """A WITH NO DATA build is a legitimate first run against a new database.

    The view exists and reports columns, but any SELECT against it raises, so the
    metrics collector has to detect that state rather than crash on it.
    """
    engine = synthetic_schemas
    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{BASELINE_SCHEMA}"'))
        conn.execute(
            sa.text(
                f'CREATE MATERIALIZED VIEW "{BASELINE_SCHEMA}".empty_mv AS '
                "SELECT 1 AS person_id, 2 AS episode_id WITH NO DATA"
            )
        )

    assert not _common.matview_is_populated(engine, BASELINE_SCHEMA, "empty_mv")
    # Columns are still introspectable, which is why the populated check is needed
    # as well as the presence check.
    assert _common.matview_columns(engine, BASELINE_SCHEMA, "empty_mv") == (
        "person_id",
        "episode_id",
    )

    with engine.begin() as conn:
        conn.execute(sa.text(f'REFRESH MATERIALIZED VIEW "{BASELINE_SCHEMA}".empty_mv'))
    assert _common.matview_is_populated(engine, BASELINE_SCHEMA, "empty_mv")
