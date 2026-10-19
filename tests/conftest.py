"""Global pytest configurations and shared distributed session fixtures."""

from __future__ import annotations

from typing import Any, Generator

import pytest


@pytest.fixture(scope="function")
def spark_session() -> Generator[Any, None, None]:
    """Provides a bounded local JVM session, stopped even after a test failure."""
    SparkSession = pytest.importorskip("pyspark.sql").SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("FlintCoreTestContext")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        yield spark
    finally:
        spark.stop()
