"""Explicit Spark session ownership and materialized, run-owned frame validation."""

import os

from flint_core.runtime.errors import DataValidationError


def open_session(spark=None, *, local=False):
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
    except ImportError:
        raise DataValidationError("Install the spark-delta extra for Spark execution.") from None
    if spark is not None and local:
        raise DataValidationError("Choose a borrowed Spark session or explicit local creation, not both.")
    if spark is not None:
        if not isinstance(spark, SparkSession) or spark.sparkContext._jsc is None:
            raise DataValidationError("A live classic Spark session is required.")
        master = spark.sparkContext.master
        if master != "local" and not (master.startswith("local[") and master.endswith("]")):
            raise DataValidationError(
                "Local Delta targets require a local Spark master, not a remote executor filesystem."
            )
        return spark, False
    if not local:
        raise DataValidationError("Pass a Spark session explicitly or request spark_local=True.")
    if SparkContext._active_spark_context is not None:
        raise DataValidationError("An existing Spark context must be passed explicitly; Flint will not adopt it.")
    if any(name in os.environ for name in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED")):
        raise DataValidationError("Local session creation cannot adopt Spark Connect environment settings.")
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        raise DataValidationError("Install the spark-delta extra for local Delta session creation.") from None
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("FlintRuntimeV1")
        .config("spark.ui.enabled", "false")
        .config("spark.api.mode", "classic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    configured = configure_spark_with_delta_pip(builder)
    try:
        with SparkSession.Builder._lock, SparkContext._lock:
            if SparkContext._active_spark_context is not None:
                raise DataValidationError("Another Spark context was created; pass it explicitly instead.")
            return configured.getOrCreate(), True
    except DataValidationError:
        raise
    except Exception:
        raise DataValidationError(
            "Local Spark creation failed; inspect the process before recovery.", cleanup_pending=True
        ) from None


def quoted(name):
    return "`" + name.replace("`", "``") + "`"


class SparkFrames:
    def __init__(self, session):
        self.session = session
        self._owned = {}

    def validate(self, frame, definition):
        from pyspark.sql import DataFrame, functions
        from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType

        if (
            not isinstance(frame, DataFrame)
            or frame.isStreaming
            or frame.sparkSession.sparkContext is not self.session.sparkContext
        ):
            raise DataValidationError("A batch DataFrame belonging to the explicit Spark context is required.")
        retained = self._owned.get(id(frame))
        if retained is not None and retained[2] is definition:
            return frame
        names = frame.columns
        declared = definition.schema.columns
        if any(not isinstance(name, str) or not name.strip() for name in names) or len(set(names)) != len(names):
            raise DataValidationError("Column labels must be distinct non-empty strings.")
        if set(declared) - set(names) or (definition.schema.extra_columns == "forbid" and set(names) - set(declared)):
            raise DataValidationError("Spark columns do not match the declared contract.")
        types = {"int64": LongType, "float64": DoubleType, "boolean": BooleanType, "string": StringType}
        for field in frame.schema.fields:
            expected = declared[field.name].type if field.name in declared else "string"
            if type(field.dataType) is not types[expected]:
                raise DataValidationError("Spark types must match exactly; implicit coercion is forbidden.")
        try:
            owned = frame.localCheckpoint(eager=True)
        except Exception:
            raise DataValidationError(
                "Spark materialization failed; owned intermediate cleanup may be incomplete.", cleanup_pending=True
            ) from None
        checkpoint = owned._jdf.logicalPlan().rdd()
        self._owned[id(owned)] = (owned, checkpoint, definition)
        try:
            checks = []
            for name, column in declared.items():
                value = functions.col(quoted(name))
                missing = value.isNull()
                if column.type == "float64":
                    missing = missing | functions.isnan(value)
                    checks.append(
                        functions.max(functions.when(value.isin(float("inf"), float("-inf")), 1).otherwise(0))
                    )
                if not column.nullable:
                    checks.append(functions.max(functions.when(missing, 1).otherwise(0)))
            if checks and any(value == 1 for value in owned.agg(*checks).first()):
                raise DataValidationError("Spark values violate nullability or finiteness.")
            for constraint in definition.constraints:
                columns = [functions.col(quoted(name)) for name in constraint.columns]
                count_name = "__flint_count"
                while count_name in names:
                    count_name += "_"
                grouped = owned.groupBy(*columns).agg(functions.count(functions.lit(1)).alias(count_name))
                duplicate = grouped.where(functions.col(quoted(count_name)) > 1).limit(1).count()
                if constraint.severity != "error" or duplicate:
                    raise DataValidationError("A declared uniqueness constraint failed.")
            return owned
        except Exception:
            try:
                self.release(owned)
            except Exception:
                raise DataValidationError(
                    "Spark validation failed and owned checkpoint cleanup is incomplete.", cleanup_pending=True
                ) from None
            raise

    def copy(self, frame):
        from pyspark.sql import functions

        return frame.select(*[functions.col(quoted(name)) for name in frame.columns])

    def release(self, frame):
        retained = self._owned.get(id(frame))
        if retained is not None:
            retained[1].unpersist(True)
            self._owned.pop(id(frame), None)

    def close(self):
        pending = False
        for frame, _, _ in tuple(self._owned.values()):
            try:
                self.release(frame)
            except Exception:
                pending = True
        return pending
