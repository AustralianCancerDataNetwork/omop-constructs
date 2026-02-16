from typing import Iterable, Type
from dataclasses import dataclass
from abc import ABC, abstractmethod
from sqlalchemy import orm as so
import sqlalchemy as sa
from .materialized import MaterializedViewMixin

@dataclass(frozen=True)
class ConstructMeta:
    name: str

    # What kind of thing is this?
    kind: str = "materialized_view"  # e.g. materialized_view | view | subquery | feature_set

    # Dependency graph
    depends_on: tuple[str, ...] = ()   # names of other constructs

    # Materialisation hints
    materialized: bool = True
    refresh_policy: str | None = None  # e.g. "on_demand", "daily", "immutable"

    # Introspection / docs
    description: str | None = None
    tags: tuple[str, ...] = ()

@dataclass
class ConstructContext:
    engine: sa.Engine
    session: so.Session

class ConstructBase(ABC):
    __construct__: ConstructMeta

    @property
    def name(self) -> str:
        return self.__construct__.name

    @abstractmethod
    def build(self, ctx: ConstructContext) -> sa.Select:
        """
        Return a SQLAlchemy Select or Subquery representing this construct.
        """

    def materialized_view(self, ctx: ConstructContext) -> type | None:
        """
        Optionally return an ORM-mapped materialized view class.
        """

    @classmethod
    def is_materialized_view(cls) -> bool:
        """
        True if this class represents a materialized view construct.
        """
        return (
            issubclass(cls, MaterializedViewMixin)
            and hasattr(cls, "__mv_name__")
            and hasattr(cls, "__mv_select__")
        )    

def collect_materialized_views(constructs: Iterable[type[ConstructBase]]):
    return [c for c in constructs if c.is_materialized_view()]