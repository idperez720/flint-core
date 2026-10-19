"""Global pytest configurations and shared distributed session fixtures."""

from __future__ import annotations

from typing import Any, Generator

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--spark-delta",
        action="store_true",
        default=False,
        help="Enable real Spark Delta tests and Maven JAR resolution.",
    )


@pytest.fixture(scope="function")
def spark_session(request: pytest.FixtureRequest) -> Generator[Any, None, None]:
    """Provides a bounded local JVM session, stopped even after a test failure."""
    SparkSession = pytest.importorskip("pyspark.sql").SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("FlintCoreTestContext")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if request.config.getoption("--spark-delta"):
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
    spark = builder.getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture
def spark_delta_session(request: pytest.FixtureRequest) -> Any:
    if not request.config.getoption("--spark-delta"):
        pytest.skip("Real Spark Delta tests require --spark-delta and a compatible delta-spark installation.")
    return request.getfixturevalue("spark_session")
