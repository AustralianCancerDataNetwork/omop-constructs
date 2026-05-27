from __future__ import annotations

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_constructs.core.errors import ConstructSpecError
from omop_constructs.core.materialized import MaterializedViewMixin
from omop_constructs.core.registry import ConstructRegistry


def test_compile_check_passes_for_valid_registry():
    class Base(so.DeclarativeBase):
        pass

    class RootMV(MaterializedViewMixin, Base):
        __tablename__ = "root_mv"
        __mv_name__ = "root_mv"
        __mv_select__ = sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(101).label("person_id"),
        )
        __mv_index__ = "mv_id"
        __mv_pk__ = ["mv_id"]
        __deps__ = ()

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
        __mv_index__ = "person_id"
        __mv_pk__ = ["mv_id"]
        __deps__ = ("root_mv",)

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)
        flag: so.Mapped[str] = so.mapped_column(sa.String)

    registry = ConstructRegistry([RootMV, ChildMV])

    report = registry.compile_check()

    assert "root_mv" in report
    assert "child_mv" in report


def test_compile_check_fails_for_missing_dependency():
    class Base(so.DeclarativeBase):
        pass

    class BrokenDepMV(MaterializedViewMixin, Base):
        __tablename__ = "broken_dep_mv"
        __mv_name__ = "broken_dep_mv"
        __mv_select__ = sa.select(sa.literal(1).label("mv_id"))
        __mv_index__ = "mv_id"
        __mv_pk__ = ["mv_id"]
        __deps__ = ("missing_mv",)

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    registry = ConstructRegistry([BrokenDepMV])

    with pytest.raises(ConstructSpecError, match="unknown deps"):
        registry.compile_check()


def test_compile_check_fails_for_invalid_index_name():
    class Base(so.DeclarativeBase):
        pass

    class BrokenIndexMV(MaterializedViewMixin, Base):
        __tablename__ = "broken_index_mv"
        __mv_name__ = "broken_index_mv"
        __mv_select__ = sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(101).label("person_id"),
        )
        __mv_index__ = "missing_col"
        __mv_pk__ = ["mv_id"]
        __deps__ = ()

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    registry = ConstructRegistry([BrokenIndexMV])

    with pytest.raises(ConstructSpecError, match="invalid __mv_index__"):
        registry.compile_check()


def test_compile_check_fails_for_invalid_pk_name():
    class Base(so.DeclarativeBase):
        pass

    class BrokenPkMV(MaterializedViewMixin, Base):
        __tablename__ = "broken_pk_mv"
        __mv_name__ = "broken_pk_mv"
        __mv_select__ = sa.select(
            sa.literal(1).label("mv_id"),
            sa.literal(101).label("person_id"),
        )
        __mv_index__ = "mv_id"
        __mv_pk__ = ["missing_pk"]
        __deps__ = ()

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    registry = ConstructRegistry([BrokenPkMV])

    with pytest.raises(ConstructSpecError, match="invalid __mv_pk__ columns \\['missing_pk'\\]"):
        registry.compile_check()


def test_compile_check_fails_for_mapper_select_drift():
    class Base(so.DeclarativeBase):
        pass

    class DriftedMV(MaterializedViewMixin, Base):
        __tablename__ = "drifted_mv"
        __mv_name__ = "drifted_mv"
        __mv_select__ = sa.select(sa.literal(1).label("mv_id"))
        __mv_index__ = "mv_id"
        __mv_pk__ = ["mv_id"]
        __deps__ = ()

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)
        person_id: so.Mapped[int] = so.mapped_column(sa.Integer)

    registry = ConstructRegistry([DriftedMV])

    with pytest.raises(ConstructSpecError, match="missing mapped columns"):
        registry.compile_check()


def test_compile_check_includes_compile_failure_details():
    class Base(so.DeclarativeBase):
        pass

    class BrokenSelect:
        def compile(self, *args, **kwargs):
            raise RuntimeError("boom")

    class BrokenCompileMV(MaterializedViewMixin, Base):
        __tablename__ = "broken_compile_mv"
        __mv_name__ = "broken_compile_mv"
        __mv_select__ = BrokenSelect()
        __mv_index__ = "mv_id"
        __mv_pk__ = ["mv_id"]
        __deps__ = ()

        mv_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    registry = ConstructRegistry([BrokenCompileMV])

    with pytest.raises(ConstructSpecError, match="RuntimeError: boom"):
        registry.compile_check()
