# Running the test suite

## Quick start

```bash
# Unit tests only
uv run --extra dev python -m pytest -m "not postgres"

# PostgreSQL integration test
export ENGINE_CDM='postgresql+psycopg://user:pass@localhost:5432/dbname'
uv run --extra dev python -m pytest -m postgres -v

# Registry schema snapshot artifact
uv run --directory . omop-constructs schema-snapshot tests/artifacts/construct_registry_schema.csv

# Equivalent module invocation
python -m omop_constructs.core.schema_snapshot tests/artifacts/construct_registry_schema.csv

# Regenerate the checked-in full-registry artifact using the Postgres fixture
UPDATE_REGISTRY_SCHEMA_SNAPSHOT=1 \
uv run --extra dev python -m pytest tests/test_registry_schema_artifact_postgres.py -m postgres -q
```

## PostgreSQL integration test

The `postgres`-marked test is opt-in and requires a reachable PostgreSQL
database. The fixture resolves the engine in this order:

1. `ENGINE_CDM`
2. `ENGINE`

During the test, `ENGINE` is populated automatically from `ENGINE_CDM` when
needed so the scratch database is visible to resolver-backed construct imports.

The test does not modify the database named in `ENGINE_CDM` / `ENGINE`
directly. Instead it:

1. connects to that server
2. creates a disposable scratch database with a generated unique name
3. bootstraps the OMOP tables there
4. runs the full registry compile check
5. creates all materialized views with `WITH NO DATA`
6. drops the scratch database on teardown

If a generated scratch database name already exists, the fixture fails instead
of dropping a pre-existing database during setup.
