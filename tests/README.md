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
database. The fixture resolves the integration source in this order:

1. the configured `cdm_db` resource from `oa-configurator`
2. `ENGINE_CDM`
3. `ENGINE`

When a stack config is present, the fixture reuses the effective `cdm_db`
schema specification and writes a temporary stack config that points `cdm_db`
at the disposable scratch database. Resolver-backed construct imports therefore
exercise the same `oa-configurator` path as normal runtime code while remaining
isolated from the configured database.

The test does not modify the configured source database directly. Instead it:

1. connects to that server
2. creates a disposable scratch database with a generated unique name
3. bootstraps the OMOP tables into the configured CDM and vocabulary schemas
4. runs the full registry compile check
5. creates all materialized views with `WITH NO DATA`
6. drops the scratch database on teardown

If a generated scratch database name already exists, the fixture fails instead
of dropping a pre-existing database during setup.
