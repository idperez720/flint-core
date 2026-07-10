"""PySpark concrete engine implementation for multi-format distributed data."""

from __future__ import annotations

import abc
import logging
import re
import threading
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Set, Type

import pyspark.sql.functions as F
from pyspark.sql import Column, SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.core.exceptions import (
    ColumnValidationError,
    UnsupportedBackendError,
)
from flint_core.spark_core.deduplication import SparkDeduplicationMixin
from flint_core.spark_core.scd2 import SparkSCD2Mixin

logger = logging.getLogger(__name__)


# =============================================================================
# BASE FORMAT STRATEGY INTERFACE
# =============================================================================


class SparkFormatHandler(abc.ABC):
    """Abstract Base Class governing format-specific distributed operations."""

    __slots__ = ()
    format_key: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers inheriting formats into global registry pools."""
        super().__init_subclass__(**kwargs)
        fmt = getattr(cls, "format_key", "").strip().lower()
        if fmt:
            SparkEngine.register_custom_format(fmt, cls)

    @abc.abstractmethod
    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        """Reads data from the specified path using the provided reader."""
        pass

    @abc.abstractmethod
    def write(self, writer: DataFrameWriter, path: str) -> None:
        """Writes the DataFrame using the provided writer configuration."""
        pass


class SparkDatabaseFormatHandler(SparkFormatHandler, abc.ABC):
    """Abstract class bridging Spark relational pipelines via JDBC drivers."""

    __slots__ = ()
    # NEW: Control flag to differentiate legitimate JDBC from in-memory catalog tables
    prefer_catalog_table: bool = False

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        # Check the flag before routing the execution flow to session.table()
        if getattr(self, "prefer_catalog_table", False) and "." in path and "/" not in path:
            session = SparkSession.getActiveSession()
            if session is not None:
                return session.table(path)
        return reader.format("jdbc").option("dbtable", path).load()

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.format("jdbc").option("dbtable", path).save()


# =============================================================================
# CORE ENTERPRISE SPARK EXECUTION ENGINE
# =============================================================================


class SparkEngine(SparkDeduplicationMixin, SparkSCD2Mixin, BaseEngine[SparkDataFrame]):
    """Enterprise PySpark engine orchestrating advanced multi-format schemas.

    Handles explicit struct field schema building, in-memory date schema string
    conversions fallbacks, and multi-cloud credential injections contexts.
    """

    __slots__ = ()

    SPARK_TYPE_MAP: ClassVar[Dict[str, DataType]] = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "decimal": DecimalType(38, 18),
    }

    FORMAT_REGISTRY: ClassVar[Dict[str, Type[SparkFormatHandler]]] = {}

    _REGISTRY_LOCK: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def register_custom_format(cls, format_name: str, handler_class: Type[SparkFormatHandler]) -> None:
        """Allows external plug-ins to inject custom formats into the engine."""
        with cls._REGISTRY_LOCK:
            cls.FORMAT_REGISTRY[format_name.strip().lower()] = handler_class
        logger.debug(
            "Successfully bound format strategy '%s' to key '%s'",
            handler_class.__name__,
            format_name,
        )

    def _inject_infrastructure(self, session: SparkSession, metadata: Optional[Mapping[str, Any]]) -> None:
        """Injects cloud infrastructure storage credentials safely into Spark."""
        if not metadata:
            return
        infra_opts = metadata.get("infrastructure", {})
        if not isinstance(infra_opts, dict):
            return

        for k, v in infra_opts.items():
            val_str = str(v)
            spark_key = k if k.startswith("spark.hadoop.") else f"spark.hadoop.{k}"
            session.conf.set(spark_key, val_str)

            jsc = getattr(session, "_jsc", None)
            if jsc is not None:
                hadoop_conf = getattr(jsc, "hadoopConfiguration", None)
                if hadoop_conf is not None and callable(hadoop_conf):
                    hadoop_conf().set(k[13:] if k.startswith("spark.hadoop.") else k, val_str)  # type: ignore
            else:
                logger.debug("JVM Gateway context is missing. Skipping JVM fallback.")

    def _resolve_format_handler(self, data_format: str) -> SparkFormatHandler:
        """Resolves concrete format strategies dynamically from the registry."""
        fmt_clean = data_format.strip().lower()
        with self._REGISTRY_LOCK:
            handler_class = self.FORMAT_REGISTRY.get(fmt_clean)

        if not handler_class:
            raise UnsupportedBackendError(
                f"No storage strategy registered for format: '{data_format}'. "
                f"Supported options: {list(self.FORMAT_REGISTRY.keys())}"
            )
        return handler_class()

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> SparkDataFrame:
        """Loads distributed data formats enforcing catalog structural schemas."""
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError("No active distributed SparkSession could be resolved.")

        self._inject_infrastructure(session, metadata)
        handler = self._resolve_format_handler(data_format)

        fields = []
        lazy_projections: List[Column] = []
        fmt = data_format.strip().lower()

        for col in columns:
            if col.data_type is None:
                fields.append(StructField(col.name, StringType(), True))
                lazy_projections.append(F.col(col.name))
                continue

            dt_clean = col.data_type.strip().lower()

            if fmt in ("csv", "json") and (dt_clean in ("date", "timestamp")) and col.format:
                fields.append(StructField(col.name, StringType(), True))
                if dt_clean == "date":
                    lazy_projections.append(F.to_date(F.col(col.name), col.format).alias(col.name))
                else:
                    lazy_projections.append(F.to_timestamp(F.col(col.name), col.format).alias(col.name))
                continue

            if dt_clean.startswith("decimal"):
                match = re.match(r"decimal\((\d+),?\s*(\d+)\)", dt_clean)
                s_type: DataType = (
                    DecimalType(int(match.group(1)), int(match.group(2))) if match else DecimalType(38, 18)
                )
            else:
                s_type = self.SPARK_TYPE_MAP.get(dt_clean, StringType())

            fields.append(StructField(col.name, s_type, True))
            lazy_projections.append(F.col(col.name))

        spark_schema = StructType(fields) if fields else None
        reader = session.read

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                spark_opts = opts.copy()
                version = spark_opts.pop("versionAsOf", None)
                as_of = spark_opts.pop("timestampAsOf", None)

                if version is not None:
                    spark_opts["versionAsOf"] = str(version)
                if as_of is not None:
                    spark_opts["timestampAsOf"] = str(as_of)

                reader = reader.options(**spark_opts)

        df = handler.read(reader, path, schema=spark_schema)

        if lazy_projections and fmt not in (
            "delta",
            "iceberg",
            "postgres",
            "postgresql",
            "mysql",
            "duckdb",
            "sqlite",
            "databricks",
            "snowflake",
            "bigquery",
        ):
            df = df.select(*lazy_projections)

        return df

    def save(
        self,
        df: SparkDataFrame,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> None:
        """Saves a distributed Spark DataFrame with schema verification."""
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError("No active distributed SparkSession could be resolved.")

        self._inject_infrastructure(session, metadata)
        handler = self._resolve_format_handler(data_format)
        writer = df.write.mode(mode)

        if metadata and metadata.get("replace_where"):
            writer = writer.option("replaceWhere", metadata["replace_where"])

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                writer = writer.options(**opts)

        if columns and data_format.strip().lower() not in (
            "delta",
            "iceberg",
            "postgres",
            "postgresql",
            "mysql",
            "duckdb",
            "sqlite",
            "databricks",
            "snowflake",
            "bigquery",
        ):
            catalog_names = [col.name for col in columns]
            input_cols: Set[str] = set(df.columns)
            missing_cols = [c for c in catalog_names if c not in input_cols]

            if missing_cols:
                raise ColumnValidationError(
                    f"Schema validation assertion failed on save phase. "
                    f"Missing expected catalog columns: {missing_cols}"
                )
            df = df.select(*catalog_names)

        handler.write(writer, path)


# =============================================================================
# CONCRETE FORMAT STRATEGIES
# =============================================================================


class CSVFormatHandler(SparkFormatHandler):
    """Strategy handler for delimited text files."""

    __slots__ = ()
    format_key: ClassVar[str] = "csv"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.csv(path, schema=schema)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.csv(path)


class ParquetFormatHandler(SparkFormatHandler):
    """Strategy handler for self-describing columnar Parquet files."""

    __slots__ = ()
    format_key: ClassVar[str] = "parquet"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.parquet(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.parquet(path)


class JSONFormatHandler(SparkFormatHandler):
    """Strategy handler for semi-structured JSON lines datasets."""

    __slots__ = ()
    format_key: ClassVar[str] = "json"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.json(path, schema=schema)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.json(path)


class ORCFormatHandler(SparkFormatHandler):
    """Strategy handler for Optimized Row Columnar deployments."""

    __slots__ = ()
    format_key: ClassVar[str] = "orc"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.orc(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.orc(path)


class DeltaFormatHandler(SparkFormatHandler):
    """Strategy handler for distributed Delta Lake transactional tables."""

    __slots__ = ()
    format_key: ClassVar[str] = "delta"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        if "." in path and "/" not in path:
            session = SparkSession.getActiveSession()
            return session.table(path)
        return reader.format("delta").load(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.format("delta").save(path)


class IcebergFormatHandler(SparkFormatHandler):
    """Strategy handler for distributed Apache Iceberg open-source tables."""

    __slots__ = ()
    format_key: ClassVar[str] = "iceberg"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.format("iceberg").load(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.format("iceberg").save(path)


class PostgresFormatHandler(SparkDatabaseFormatHandler):
    format_key = "postgres"


class PostgresqlFormatHandler(SparkDatabaseFormatHandler):
    format_key = "postgresql"


class MySQLFormatHandler(SparkDatabaseFormatHandler):
    format_key = "mysql"


class DuckDBFormatHandler(SparkDatabaseFormatHandler):
    format_key = "duckdb"


class SQLiteFormatHandler(SparkDatabaseFormatHandler):
    format_key = "sqlite"


class DatabricksFormatHandler(SparkDatabaseFormatHandler):
    format_key = "databricks"
    prefer_catalog_table = True


class SnowflakeFormatHandler(SparkFormatHandler):
    """Strategy handler for Snowflake cloud warehouse connections."""

    __slots__ = ()
    format_key: ClassVar[str] = "snowflake"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.format("net.snowflake.spark.snowflake").option("dbtable", path).load()

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.format("net.snowflake.spark.snowflake").option("dbtable", path).save()


class BigQueryFormatHandler(SparkFormatHandler):
    """Strategy handler for Google BigQuery ecosystem connections."""

    __slots__ = ()
    format_key: ClassVar[str] = "bigquery"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.format("bigquery").option("table", path).load()

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.format("bigquery").option("table", path).save()


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
