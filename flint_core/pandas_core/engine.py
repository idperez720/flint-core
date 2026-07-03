"""Pandas concrete engine implementation for multi-format data interaction."""

from __future__ import annotations

import abc
import decimal
import logging
import threading
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Set, Tuple, Type
from urllib.parse import urlparse

import pandas as pd

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.core.exceptions import (
    ColumnValidationError,
    UnsupportedBackendError,
)
from flint_core.pandas_core.deduplication import PandasDeduplicationMixin
from flint_core.pandas_core.scd2 import PandasSCD2Mixin

logger = logging.getLogger(__name__)


# =============================================================================
# BASE FORMAT STRATEGY INTERFACE
# =============================================================================


class PandasFormatHandler(abc.ABC):
    """Abstract Base Class governing local format-specific operations."""

    __slots__ = ()
    format_key: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers inheriting formats into the global engine."""
        super().__init_subclass__(**kwargs)
        fmt = getattr(cls, "format_key", "").strip().lower()
        if fmt:
            PandasEngine.register_custom_format(fmt, cls)

    @abc.abstractmethod
    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        """Reads local or remote cloud data into a standard pandas DataFrame."""
        pass

    @abc.abstractmethod
    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        """Writes pandas DataFrames onto persistent system storage layers."""
        pass


# =============================================================================
# CORE ENTERPRISE PANDAS RUNTIME ENGINE
# =============================================================================


class PandasEngine(PandasDeduplicationMixin, PandasSCD2Mixin, BaseEngine[pd.DataFrame]):
    """Unified Pandas engine orchestrating agnostically decoupled multi-format inputs."""

    __slots__ = ()

    PANDAS_TYPE_MAP: ClassVar[Dict[str, str]] = {
        "integer": "Int64",
        "string": "str",
        "double": "float64",
        "float": "float32",
        "boolean": "bool",
    }

    FORMAT_REGISTRY: ClassVar[Dict[str, Type[PandasFormatHandler]]] = {}

    _REGISTRY_LOCK: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def register_custom_format(cls, format_name: str, handler_class: Type[PandasFormatHandler]) -> None:
        """Safe thread-secured Inversion of Control format expansion hook."""
        with cls._REGISTRY_LOCK:
            cls.FORMAT_REGISTRY[format_name.strip().lower()] = handler_class
        logger.debug(
            "Successfully bound pandas format strategy '%s' to key '%s'",
            handler_class.__name__,
            format_name,
        )

    def _resolve_format_handler(self, data_format: str) -> PandasFormatHandler:
        """Resolves concrete format strategies dynamically from the registry."""
        fmt_clean = data_format.strip().lower()
        with self._REGISTRY_LOCK:
            handler_class = self.FORMAT_REGISTRY.get(fmt_clean)

        if not handler_class:
            raise UnsupportedBackendError(
                f"No storage strategy registered inside Pandas engine for format: "
                f"'{data_format}'. Supported options: "
                f"{list(self.FORMAT_REGISTRY.keys())}"
            )
        return handler_class()

    def _compile_primitive_schema_parameters(self, columns: List[ColumnDefinition]) -> Tuple[Dict[str, Any], List[str]]:
        """Compiles primitive dtype dictionaries and structural date trackers."""
        dtype_dict: Dict[str, Any] = {}
        parse_dates_fallback: List[str] = []

        for col in columns:
            if col.data_type is None:
                continue
            dt_clean = col.data_type.strip().lower()

            if dt_clean == "timestamp" and not col.format:
                parse_dates_fallback.append(col.name)
            elif dt_clean in self.PANDAS_TYPE_MAP:
                dtype_dict[col.name] = self.PANDAS_TYPE_MAP[dt_clean]

        return dtype_dict, parse_dates_fallback

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> pd.DataFrame:
        """Loads data into a local or cloud Pandas DataFrame with unified options."""
        options = dict(metadata.get("options", {})) if metadata else {}

        if metadata and "infrastructure" in metadata:
            infra = metadata["infrastructure"]
            if isinstance(infra, dict) and "storage_options" not in options:
                options["storage_options"] = {str(k): v for k, v in infra.items()}

        handler = self._resolve_format_handler(data_format)
        fmt_clean = data_format.strip().lower()

        dtype_dict, parse_dates_fallback = self._compile_primitive_schema_parameters(columns)

        df = handler.read(path, dtype_dict, parse_dates_fallback, options)

        if fmt_clean in ("parquet", "orc"):
            df = self._apply_primitive_dtypes(df, dtype_dict)

        return self._enforce_rich_types(df, columns, parse_dates_fallback)

    def save(
        self,
        df: pd.DataFrame,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Saves a local or cloud Pandas DataFrame executing strict validations."""
        options = dict(metadata.get("options", {})) if metadata else {}

        if metadata and "infrastructure" in metadata:
            infra = metadata["infrastructure"]
            if isinstance(infra, dict) and "storage_options" not in options:
                options["storage_options"] = {str(k): v for k, v in infra.items()}

        handler = self._resolve_format_handler(data_format)
        is_cloud = urlparse(path).scheme in ("s3", "s3a", "s3n", "gs", "gcs", "abfss", "az")

        if not is_cloud:
            file_path = Path(path)
            if file_path.exists():
                if mode == "error":
                    raise FileExistsError(f"Target path already exists locally: '{path}'.")
                if mode == "ignore":
                    return

            if not file_path.parent.exists():
                file_path.parent.mkdir(parents=True, exist_ok=True)

        if not columns:
            df_enforced = df.copy()
        else:
            catalog_names = [col.name for col in columns]
            input_cols: Set[str] = set(df.columns)
            missing_cols = [c for c in catalog_names if c not in input_cols]

            if missing_cols:
                raise ColumnValidationError(
                    f"Schema enforcement validation failed during save. "
                    f"Missing expected catalog columns: {missing_cols}"
                )

            df_enforced = df[catalog_names].copy()
            dtype_dict, parse_dates_fallback = self._compile_primitive_schema_parameters(columns)

            df_enforced = self._apply_primitive_dtypes(df_enforced, dtype_dict)
            df_enforced = self._enforce_rich_types(df_enforced, columns, parse_dates_fallback)

        handler.write(df_enforced, path, options)

    def _apply_primitive_dtypes(self, df: pd.DataFrame, dtype_dict: Dict[str, Any]) -> pd.DataFrame:
        """Applies primitive data types safely onto an existing DataFrame."""
        for col_name, dtype_val in dtype_dict.items():
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(dtype_val)
        return df

    def _enforce_rich_types(
        self,
        df: pd.DataFrame,
        columns: List[ColumnDefinition],
        fallbacks: List[str],
    ) -> pd.DataFrame:
        """Enforces column-specific advanced business formats and timezones."""
        for col in columns:
            if col.data_type is None or col.name not in df.columns:
                continue
            dt_clean = col.data_type.strip().lower()

            if dt_clean == "timestamp":
                if col.format:
                    df[col.name] = pd.to_datetime(df[col.name], format=col.format)
                elif col.name not in fallbacks:
                    df[col.name] = pd.to_datetime(df[col.name])

                if col.timezone:
                    dt_series = df[col.name].dt
                    if dt_series.tz is None:
                        df[col.name] = dt_series.tz_localize(col.timezone)
                    else:
                        df[col.name] = dt_series.tz_convert(col.timezone)

            elif dt_clean == "date":
                df[col.name] = pd.to_datetime(df[col.name], format=col.format if col.format else None).dt.date

            elif dt_clean.startswith("decimal"):
                df[col.name] = pd.Series(
                    [decimal.Decimal(str(x)) if pd.notnull(x) else None for x in df[col.name]],
                    index=df.index,
                    dtype="object",
                )

        return df


# =============================================================================
# CONCRETE FORMAT STRATEGIES
# =============================================================================


class CSVFormatHandler(PandasFormatHandler):
    """Strategy handler for delimited text files."""

    __slots__ = ()
    format_key: ClassVar[str] = "csv"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        return pd.read_csv(
            path,
            dtype=dtype_dict if dtype_dict else None,  # type: ignore
            parse_dates=parse_dates if parse_dates else None,
            **options,
        )  # type: ignore

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        opts = options.copy()
        index_val = opts.pop("index", False)
        df.to_csv(path, index=index_val, **opts)


class ParquetFormatHandler(PandasFormatHandler):
    """Strategy handler for self-describing columnar Parquet structures."""

    __slots__ = ()
    format_key: ClassVar[str] = "parquet"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        return pd.read_parquet(path, **options)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        df.to_parquet(path, **options)


class JSONFormatHandler(PandasFormatHandler):
    """Strategy handler for semi-structured JSON lines datasets."""

    __slots__ = ()
    format_key: ClassVar[str] = "json"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        opts = options.copy()
        orient_val = opts.pop("orient", "records")
        return pd.read_json(path, orient=orient_val, dtype=dtype_dict, **opts)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        opts = options.copy()
        orient_val = opts.pop("orient", "records")
        df.to_json(path, orient=orient_val, **opts)


class ORCFormatHandler(PandasFormatHandler):
    """Strategy handler for Optimized Row Columnar deployments."""

    __slots__ = ()
    format_key: ClassVar[str] = "orc"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        return pd.read_orc(path, **options)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        df.to_orc(path, **options)


class DeltaFormatHandler(PandasFormatHandler):
    """Strategy handler for Delta Lake tables using deltalake bindings."""

    __slots__ = ()
    format_key: ClassVar[str] = "delta"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        try:
            import deltalake
        except ImportError as e:
            raise UnsupportedBackendError("The 'deltalake' library is required to read delta format.") from e

        opts = options.copy()
        version = opts.pop("versionAsOf", None)
        as_of = opts.pop("timestampAsOf", None)

        dt = deltalake.DeltaTable(path, version=version, datetime=as_of, **opts)
        return dt.to_pandas()

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        try:
            import deltalake
        except ImportError as e:
            raise UnsupportedBackendError("The 'deltalake' library is required to write delta format.") from e

        opts = options.copy()
        mode = opts.pop("mode", "append")
        deltalake.write_deltalake(path, df, mode=mode, **opts)


class IcebergFormatHandler(PandasFormatHandler):
    """Strategy handler for Apache Iceberg tables using pyiceberg integration."""

    __slots__ = ()
    format_key: ClassVar[str] = "iceberg"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise UnsupportedBackendError("The 'pyiceberg' library is required to read iceberg format.") from e

        opts = options.copy()
        catalog_name = opts.pop("catalog_name", "default")
        catalog = load_catalog(catalog_name, **opts)
        table = catalog.load_table(path)

        version = opts.pop("versionAsOf", None)
        if version is not None:
            return table.scan(snapshot_id=int(version)).to_pandas()
        return table.scan().to_pandas()

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise UnsupportedBackendError("The 'pyiceberg' library is required to write iceberg format.") from e
        opts = options.copy()
        catalog_name = opts.pop("catalog_name", "default")
        catalog = load_catalog(catalog_name, **opts)
        table = catalog.load_table(path)
        table.append(df)


# Initialize internal default format definitions seamlessly through side-effects
_DEFAULTS = [
    CSVFormatHandler,
    ParquetFormatHandler,
    JSONFormatHandler,
    ORCFormatHandler,
    DeltaFormatHandler,
    IcebergFormatHandler,
]
