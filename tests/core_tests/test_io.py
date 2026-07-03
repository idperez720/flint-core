"""Unit tests for multi-format DataLoader and DataSaver engine enforcement."""

from __future__ import annotations

import decimal
from pathlib import Path
from typing import Any, Generator
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import UnsupportedBackendError
from flint_core.core.io import DataLoader, DataSaver


@pytest.fixture
def mock_advanced_io_environment(
    tmp_path: Path,
) -> Generator[DataCatalog, None, None]:
    """Scaffolds a comprehensive environment with nested options for testing."""
    catalog_dir = tmp_path / "conf" / "catalog"
    data_dir = tmp_path / "data"

    catalog_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-advanced-io"\n')

    csv_path = data_dir / "dataset.csv"
    csv_content = "id;price\n1;99.99\n2;150.50\n"
    csv_path.write_text(csv_content, encoding="utf-8")

    catalog_yaml = {
        "csv_dataset": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {"sep": ";", "encoding": "utf-8"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "price", "type": "decimal(10,2)"},
            ],
        },
        "pandas_strict_save": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/output/strict_pandas.csv",
            "options": {"sep": "|"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "value", "type": "string"},
            ],
        },
        "pandas_schemaless_save": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/output/schemaless_pandas.csv",
            "options": {"sep": "|"},
        },
        "spark_csv_dataset": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {"delimiter": ";"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "price", "type": "decimal(10,2)"},
            ],
        },
        "pandas_cloud_s3": {
            "engine": "pandas",
            "format": "parquet",
            "storage_path": "s3://remote-bucket/analytics/data.parquet",
        },
        "pandas_cloud_azure": {
            "engine": "pandas",
            "format": "parquet",
            "storage_path": "abfss://container@storage.dfs.core.windows.net/file.pq",
        },
        "spark_cloud_dataset": {
            "engine": "spark",
            "format": "parquet",
            "storage_path": "gcs://datacore/gold/orders/",
            "infrastructure": {
                "fs.gs.impl": ("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"),
                "fs.azure.account.key.dummy": "secret-token",
            },
        },
        "unsupported_engine": {
            "engine": "duckdb",
            "format": "parquet",
            "storage_path": "data/dataset.parquet",
        },
    }

    with open(catalog_dir / "advanced_io.yaml", "w", encoding="utf-8") as f:
        yaml.dump(catalog_yaml, f)

    yield DataCatalog(catalog_path=catalog_dir)


def test_pandas_csv_loading_options_and_enforcement(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts option pass-through and column-level advanced types for CSV."""
    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_advanced_io_environment)
    df = loader.load("csv_dataset")

    assert isinstance(df, pd.DataFrame)
    assert df["id"].dtype == "Int64"
    assert isinstance(df["price"].iloc[0], decimal.Decimal)


def test_pandas_saver_strict_drops_extra_columns(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that strict saving filters and truncates extra columns."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"id": [1], "value": ["A"], "extra_untracked_field": [999]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "pandas_strict_save", mode="overwrite")

    root = mock_advanced_io_environment.project_root
    expected_file = Path(root) / "data/output/strict_pandas.csv"

    read_back = pd.read_csv(expected_file, sep="|")
    assert list(read_back.columns) == ["id", "value"]


def test_pandas_saver_schemaless_keeps_all_columns(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that schemaless saving bypasses column truncation filters."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"dynamic_id": [5], "custom_metric": [10.5], "tag": ["alpha"]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "pandas_schemaless_save", mode="overwrite")

    root = mock_advanced_io_environment.project_root
    expected_file = Path(root) / "data/output/schemaless_pandas.csv"

    read_back = pd.read_csv(expected_file, sep="|")
    assert list(read_back.columns) == ["dynamic_id", "custom_metric", "tag"]


def test_pandas_data_saver_collision_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that save mode='error' raises on pre-existing paths."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"id": [1]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    with pytest.raises(FileExistsError):
        saver.save(test_df, "csv_dataset", mode="error")


def test_pandas_data_saver_mode_ignore(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that save mode='ignore' skips write execution silently."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"id": [999]})

    saver = DataSaver(catalog=mock_advanced_io_environment)

    with patch("pandas.DataFrame.to_csv") as mock_writer:
        saver.save(test_df, "csv_dataset", mode="ignore")
        mock_writer.assert_not_called()


def test_unsupported_engine_raises_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that an unregistered engine throws UnsupportedBackendError."""
    loader = DataLoader(catalog=mock_advanced_io_environment)
    with pytest.raises(UnsupportedBackendError):
        loader.load("unsupported_engine")


def test_pandas_saver_cloud_s3_path_bypass(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that saving to an S3 URI bypasses system path checks."""
    pd = pytest.importorskip("pandas")
    dummy_df = pd.DataFrame({"id": [1]})

    saver = DataSaver(catalog=mock_advanced_io_environment)

    with patch("pandas.DataFrame.to_parquet") as mock_writer:
        saver.save(dummy_df, "pandas_cloud_s3", mode="overwrite")
        mock_writer.assert_called_once_with("s3://remote-bucket/analytics/data.parquet")


def test_pandas_saver_cloud_azure_path_bypass(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that saving to an Azure ABFSS URI bypasses local path validation."""
    pd = pytest.importorskip("pandas")
    dummy_df = pd.DataFrame({"id": [1]})

    saver = DataSaver(catalog=mock_advanced_io_environment)

    with patch("pandas.DataFrame.to_parquet") as mock_writer:
        saver.save(dummy_df, "pandas_cloud_azure", mode="overwrite")
        mock_writer.assert_called_once_with("abfss://container@storage.dfs.core.windows.net/file.pq")


def test_spark_engine_infrastructure_injection(mock_advanced_io_environment: DataCatalog, spark_session: Any) -> None:
    """Asserts prefix validation and mapping inside active Spark contexts."""
    pytest.importorskip("pyspark")
    loader = DataLoader(catalog=mock_advanced_io_environment)

    with patch("pyspark.sql.DataFrameReader.parquet") as mock_reader:
        try:
            loader.load("spark_cloud_dataset", spark=spark_session)
        except Exception:
            pass

        # Already had spark.hadoop prefix, should be intact
        assert (
            spark_session.conf.get("spark.hadoop.fs.gs.impl") == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        # Missing prefix, should have been injected automatically by framework
        assert spark_session.conf.get("spark.hadoop.fs.azure.account.key.dummy") == "secret-token"


def test_pandas_engine_cloud_infrastructure_injection(tmp_path: Path) -> None:
    """Asserts that infrastructure definitions map directly to fsspec storage options."""
    pd = pytest.importorskip("pandas")
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-pandas-cloud"\n')

    cloud_catalog = {
        "pandas_s3_secured": {
            "engine": "pandas",
            "format": "parquet",
            "storage_path": "s3://secure-bucket/raw/events.parquet",
            "infrastructure": {
                "key": "aws-access-key-id",
                "secret": "aws-secret-access-key",
            },
        }
    }

    with open(catalog_dir / "cloud.yaml", "w", encoding="utf-8") as f:
        yaml.dump(cloud_catalog, f)

    catalog = DataCatalog(catalog_path=catalog_dir)
    loader = DataLoader(catalog=catalog)

    with patch("pandas.read_parquet") as mock_parquet_reader:
        try:
            loader.load("pandas_s3_secured")
        except Exception:
            pass

        # Verify that storage_options were implicitly channeled down to fsspec
        mock_parquet_reader.assert_called_once()
        kwargs = mock_parquet_reader.call_args[1]
        assert "storage_options" in kwargs
        assert kwargs["storage_options"]["key"] == "aws-access-key-id"
        assert kwargs["storage_options"]["secret"] == "aws-secret-access-key"
