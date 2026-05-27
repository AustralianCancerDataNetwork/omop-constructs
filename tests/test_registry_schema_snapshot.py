from __future__ import annotations

import csv

import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_constructs.core.materialized import MaterializedViewMixin
from omop_constructs.core.registry import ConstructRegistry
from omop_constructs.core.schema_snapshot import (
    SNAPSHOT_HEADERS,
    registry_schema_rows,
    write_registry_schema_snapshot,
)


def _build_registry() -> ConstructRegistry:
    class Base(so.DeclarativeBase):
        pass

    class RootMV(MaterializedViewMixin, Base):
        __tablename__ = "root_mv"
        __mv_name__ = "root_mv"
        __mv_select__ = sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(101).label("person_id"),
        )
        __mv_index__ = "person_id"
        __mv_pk__ = ["mv_id"]
        __deps__: tuple[str, ...] = ()

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    class ChildMV(MaterializedViewMixin, Base):
        __tablename__ = "child_mv"
        __mv_name__ = "child_mv"
        __mv_select__ = sa.select(
            RootMV.mv_id.label("mv_id"),
            RootMV.person_id.label("person_id"),
            sa.literal("ok").label("flag"),
        )
        __mv_index__ = "mv_id"
        __mv_pk__ = ["mv_id"]
        __deps__: tuple[str, ...] = ("root_mv",)

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
        flag: so.Mapped[str] = so.mapped_column(sa.String)

    return ConstructRegistry([RootMV, ChildMV])


def test_registry_schema_rows_capture_construct_metadata():
    registry = _build_registry()

    rows = registry_schema_rows(registry)

    assert [row["construct_name"] for row in rows] == [
        "child_mv",
        "child_mv",
        "child_mv",
        "root_mv",
        "root_mv",
    ]
    assert rows[0]["deps"] == "root_mv"
    assert rows[0]["mv_index"] == "mv_id"
    assert rows[0]["mv_pk"] == "mv_id"
    assert rows[0]["column_position"] == "1"
    assert rows[0]["column_name"] == "mv_id"
    assert rows[0]["primary_key"] == "true"
    assert rows[1]["column_name"] == "person_id"
    assert rows[2]["column_name"] == "flag"
    assert rows[2]["column_type"] == "VARCHAR"


def test_write_registry_schema_snapshot_emits_csv(tmp_path):
    registry = _build_registry()
    output = tmp_path / "registry-schema.csv"

    path = write_registry_schema_snapshot(output, registry=registry)

    assert path == output
    with output.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames or ()) == SNAPSHOT_HEADERS
        rows = list(reader)

    assert len(rows) == 5
    assert rows[0]["construct_name"] == "child_mv"
    assert rows[-1]["construct_name"] == "root_mv"
