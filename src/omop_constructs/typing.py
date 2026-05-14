from typing import Protocol, Iterable, runtime_checkable, Any
import sqlalchemy as sa
from .core.plan import ConstructNode, topo_sort

@runtime_checkable
class SupportsMaterializedView(Protocol):
    """
    Protocol for ORM classes backed by materialized view definitions.
    """
    __mv_name__: str
    __mv_select__: sa.sql.Select
    __deps__: tuple[str, ...]
    __construct__: Any

    @classmethod
    def create_mv(cls, bind, *, with_data: bool = True) -> None: ...
    @classmethod
    def refresh_mv(cls, bind, *, concurrently: bool = False) -> None: ...
    @classmethod
    def drop_mv(cls, bind, *, cascade: bool = False) -> None: ...


@runtime_checkable
class SupportsConstructRegistry(Protocol):
    """
    Protocol for registry-like lifecycle managers.
    """
    def create_all(self, bind: Any) -> None: ...
    def refresh_all(
        self,
        bind: Any,
        *,
        concurrently: bool = False,
    ) -> None: ...
    def drop_all(self, bind: Any, *, cascade: bool = False) -> None: ...


@runtime_checkable
class SupportsSelectable(Protocol):
    """
    Protocol for objects that can emit a SQLAlchemy ``Select``.
    """
    def select(self) -> sa.sql.Select: ...


@runtime_checkable
class SupportsQueryFactory(Protocol):
    """
    Factory that produces SQLAlchemy ``Select`` objects.
    """
    def build(self) -> sa.sql.Select: ...
