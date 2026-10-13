"""Declarative metadata for constructs-backed materialized views."""

from sqlalchemy.orm import DeclarativeBase


class ConstructORMBase(DeclarativeBase):
    """Keep materialized-view mappings out of OMOP table bootstrap metadata."""
