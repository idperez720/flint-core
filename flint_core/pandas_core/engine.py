"""Pandas concrete engine implementation for multi-format data interaction."""

from __future__ import annotations

import abc
import decimal
import logging
import threading
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
)
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
    """Abstract Base Class governing local format-specific file operations."""

    __slots__ = ()
    format_key: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers inheriting formats into global registry pools."""
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


class PandasDatabaseFormatHandler(PandasFormatHandler, abc.ABC):
    """Abstract class bridging relational database engines via custom connectors."""

    __slots__ = ()

    def _get_connection(self, options: Dict[str, Any]) -> Any:
        """Resolves active connections utilizing specified protocol wrappers.

        Args:
            options: Configuration options holding endpoint details.

        Returns:
            Any: Target driver engine connection or SQLAlchemy pool.
        """
        url = options.get("url")
        connector = options.get("connector")

        if connector == "jdbc":
            try:
                import jaydebeapi
            except ImportError as e:
                raise UnsupportedBackendError("The 'jaydebeapi' package is required for JDBC connections.") from e
            return jaydebeapi.connect(
                options.get("driver"),
                url,
                options.get("credentials", []),
                options.get("jars"),
            )

        if connector == "odbc":
            try:
                import pyodbc
            except ImportError as e:
                raise UnsupportedBackendError("The 'pyodbc' package is required for ODBC connections.") from e
            dsn = options.get("dsn")
            if dsn:
                rem_keys = ("url", "dsn")
                kv_pair = [f"{k}={v}" for k, v in options.items() if k not in rem_keys]
                return pyodbc.connect(f"DSN={dsn};" + ";".join(kv_pair))
            return pyodbc.connect(url)

        from sqlalchemy import create_engine

        if not url:
            raise ValueError("Database token connection 'url' is required.")
        return create_engine(url)


# =============================================================================
# CORE ENTERPRISE PANDAS RUNTIME ENGINE
# =============================================================================


class PandasEngine(PandasDeduplicationMixin, PandasSCD2Mixin, BaseEngine[pd.DataFrame]):
    """Unified Pandas engine orchestrating dynamic decoupled format strategies.

    Manages physical type casting enforcement, date parsing parameters, and
    provides side-effect strategy hooks for cloud databases.
    """

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
                f"No storage strategy registered inside Pandas engine: "
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

        if metadata and "connector" in metadata and metadata["connector"]:
            options["connector"] = metadata["connector"]

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

        if metadata and "connector" in metadata and metadata["connector"]:
            options["connector"] = metadata["connector"]

        handler = self._resolve_format_handler(data_format)
        fmt_clean = data_format.strip().lower()

        is_relational = fmt_clean in (
            "postgres",
            "postgresql",
            "mysql",
            "duckdb",
            "sqlite",
            "snowflake",
            "bigquery",
            "databricks",
        )
        is_cloud = urlparse(path).scheme in (
            "s3",
            "s3a",
            "s3n",
            "gs",
            "gcs",
            "abfss",
            "az",
        )

        if not is_cloud and not is_relational:
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


class RelationalDBFormatHandler(PandasDatabaseFormatHandler):
    """Symmetrical concrete database proxy routing mapped relational profiles."""

    __slots__ = ()

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        conn = self._get_connection(options)
        opts = {k: v for k, v in options.items() if k not in ("url", "driver", "connector")}
        if hasattr(conn, "connect"):
            return pd.read_sql_table(path, con=conn, **opts)
        return pd.read_sql_query(f"SELECT * FROM {path}", con=conn, **opts)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        conn = self._get_connection(options)
        opts = {k: v for k, v in options.items() if k not in ("url", "driver", "connector")}
        if_exists_val = opts.pop("if_exists", "fail")
        if hasattr(conn, "connect"):
            df.to_sql(path, con=conn, if_exists=if_exists_val, index=False, **opts)
        else:
            raise UnsupportedBackendError("Bulk loading over raw connections requires SQLAlchemy.")


class PostgresFormatHandler(RelationalDBFormatHandler):
    format_key = "postgres"


class PostgresqlFormatHandler(RelationalDBFormatHandler):
    format_key = "postgresql"


class MySQLFormatHandler(RelationalDBFormatHandler):
    format_key = "mysql"


class DuckDBFormatHandler(RelationalDBFormatHandler):
    format_key = "duckdb"


class SQLiteFormatHandler(RelationalDBFormatHandler):
    format_key = "sqlite"


class DatabricksFormatHandler(PandasDatabaseFormatHandler):
    """Strategy handler for Databricks SQL Warehouses via connectors."""

    __slots__ = ()
    format_key: ClassVar[str] = "databricks"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        opts = options.copy()
        connector = opts.get("connector", "native")

        if connector == "native":
            try:
                from databricks import sql
            except ImportError as e:
                raise UnsupportedBackendError("The 'databricks-sql-connector' package is required.") from e
            server_hostname = opts.pop("server_hostname", None)
            http_path = opts.pop("http_path", None)
            access_token = opts.pop("access_token", None)
            with sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
                **opts,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {path}")
                    return cursor.fetchall_as_pandas()

        conn = self._get_connection(options)
        db_opts = {k: v for k, v in opts.items() if k not in ("url", "driver", "connector")}
        if hasattr(conn, "connect"):
            return pd.read_sql_table(path, con=conn, **db_opts)
        return pd.read_sql_query(f"SELECT * FROM {path}", con=conn, **db_opts)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        conn = self._get_connection(options)
        opts = {k: v for k, v in options.items() if k not in ("url", "driver", "connector")}
        if_exists_val = opts.pop("if_exists", "fail")
        if hasattr(conn, "connect"):
            df.to_sql(path, con=conn, if_exists=if_exists_val, index=False, **opts)
        else:
            raise UnsupportedBackendError("Writing to Databricks over JDBC requires SQLAlchemy.")


class SnowflakeFormatHandler(PandasFormatHandler):
    """Highly optimized bulk loading strategy wrapper for SnowflakeWarehouses."""

    __slots__ = ()
    format_key: ClassVar[str] = "snowflake"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        try:
            import snowflake.connector
        except ImportError as e:
            raise UnsupportedBackendError("The 'snowflake-connector-python' is required.") from e
        opts = options.copy()
        conn = snowflake.connector.connect(**opts)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {path}")
            return cursor.fetch_pandas_all()
        finally:
            cursor.close()
            conn.close()

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as e:
            raise UnsupportedBackendError("The 'snowflake-connector-python' is required.") from e
        opts = options.copy()
        conn_keys = {
            "user",
            "password",
            "account",
            "warehouse",
            "database",
            "schema",
            "role",
        }
        conn_opts = {k: opts.pop(k) for k in list(opts.keys()) if k in conn_keys}
        conn = snowflake.connector.connect(**conn_opts)
        try:
            write_pandas(conn, df, table_name=path, **opts)
        finally:
            conn.close()


class BigQueryFormatHandler(PandasFormatHandler):
    """Highly optimized bulk loading strategy wrapper for Google BigQuery."""

    __slots__ = ()
    format_key: ClassVar[str] = "bigquery"

    def read(
        self,
        path: str,
        dtype_dict: Dict[str, Any],
        parse_dates: List[str],
        options: Dict[str, Any],
    ) -> pd.DataFrame:
        try:
            import pandas_gbq
        except ImportError as e:
            raise UnsupportedBackendError("The 'pandas-gbq' library is required.") from e
        return pandas_gbq.read_gbq(path, **options)

    def write(self, df: pd.DataFrame, path: str, options: Dict[str, Any]) -> None:
        try:
            import pandas_gbq
        except ImportError as e:
            raise UnsupportedBackendError("The 'pandas-gbq' library is required.") from e
        opts = options.copy()
        if_exists_val = opts.pop("if_exists", "fail")
        pandas_gbq.to_gbq(df, path, if_exists=if_exists_val, **opts)


# Initialize internal default format definitions seamlessly through side-effects
_DEFAULTS = [
    CSVFormatHandler,
    ParquetFormatHandler,
    JSONFormatHandler,
    ORCFormatHandler,
    DeltaFormatHandler,
    IcebergFormatHandler,
    PostgresFormatHandler,
    PostgresqlFormatHandler,
    MySQLFormatHandler,
    DuckDBFormatHandler,
    SQLiteFormatHandler,
    DatabricksFormatHandler,
    SnowflakeFormatHandler,
    BigQueryFormatHandler,
]
