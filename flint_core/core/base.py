"""Foundational infrastructure for engine registration and dynamic dispatching."""

from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

from flint_core.core.exceptions import (
    UnsupportedBackendError,
)

if TYPE_CHECKING:
    from flint_core.core.catalog.models import ColumnDefinition

logger = logging.getLogger(__name__)

DataFrameT = TypeVar("DataFrameT", bound=Any)


class EngineRegistry:
    """Central registry driven by Inversion of Control to manage concrete engines."""

    _REGISTRY: ClassVar[Dict[str, Type[BaseEngine[Any]]]] = {}

    @classmethod
    def resolve_engine(cls, df: Any) -> BaseEngine[Any]:
        """Inspects the dataframe's module to dynamically dispatch the engine."""
        df_module = type(df).__module__.lower()

        for keyword, engine_cls in cls._REGISTRY.items():
            if keyword in df_module:
                return engine_cls()

        raise UnsupportedBackendError(
            f"No execution engine registered for dataframe module: "
            f"'{type(df).__module__}'. "
            f"Supported backends: {list(cls._REGISTRY.keys())}"
        )

    @classmethod
    def get_engine(cls, name: str) -> BaseEngine[Any]:
        """Resolves an engine instance by its registered string identifier."""
        engine_cls = cls._REGISTRY.get(name.lower())
        if not engine_cls:
            raise UnsupportedBackendError(
                f"No execution engine registered for name: '{name}'. Supported backends: {list(cls._REGISTRY.keys())}"
            )
        return engine_cls()


class BaseEngine(abc.ABC, Generic[DataFrameT]):
    """Abstract Base Class orchestrating automated registration via IoC."""

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers any inheriting engine subclass."""
        super().__init_subclass__(**kwargs)
        keyword = cls.__name__.replace("Engine", "").lower()
        if keyword:
            EngineRegistry._REGISTRY[keyword] = cls

    @abc.abstractmethod
    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> Any:
        """Abstract requirement for data loading and schema enforcement."""
        pass

    @abc.abstractmethod
    def save(
        self,
        df: Any,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Abstract requirement for data saving and option enforcement."""
        pass
