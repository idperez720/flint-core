"""Spark writer unit checks using the SDK without starting a JVM."""

from unittest.mock import MagicMock, patch

import pytest

from flint_core.core.catalog.models import ColumnDefinition

pytest.importorskip("pyspark")
SparkEngine = pytest.importorskip("flint_core.spark_core.engine").SparkEngine


@pytest.mark.parametrize("mode", ["error", "ignore", "overwrite", "append"])
def test_spark_uses_projected_dataframe_and_requested_mode(mode: str) -> None:
    session = MagicMock()
    dataframe = MagicMock()
    dataframe.columns = ["id", "extra"]
    projected = dataframe.select.return_value
    writer = projected.write.mode.return_value

    SparkEngine().save(dataframe, "customers.csv", "csv", [ColumnDefinition("id")], mode=mode, spark=session)

    dataframe.select.assert_called_once_with("id")
    dataframe.write.mode.assert_not_called()
    projected.write.mode.assert_called_once_with(mode)
    writer.csv.assert_called_once_with("customers.csv")


def test_spark_rejects_mode_override_before_session_resolution() -> None:
    with patch("pyspark.sql.SparkSession.getActiveSession") as resolve_session:
        with pytest.raises(ValueError, match="options"):
            SparkEngine().save(
                MagicMock(), "customers", "csv", [], mode="append", metadata={"options": {"mode": "overwrite"}}
            )
    resolve_session.assert_not_called()
