"""Unit tests for flint-core hybrid OLAP and relational database connectivity.

Validates directional routing configurations, driver instantiation factories,
and high-fidelity Spark Unity Catalog three-level namespace interception.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from flint_core.core.catalog import DataCatalog


def _setup_mock_project_environment(tmp_path: Path, catalog_data: dict) -> Path:
    """Helper method to scaffold a valid convention-based workspace anchor."""
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "flint-test-workspace"\n')

    with open(catalog_dir / "analytics.yaml", "w", encoding="utf-8") as f:
        yaml.dump(catalog_data, f)

    return catalog_dir


def test_pandas_jdbc_connector_factory(tmp_path: Path) -> None:
    """Asserts that relational engine formats map via explicit JDBC parameters."""
    pd = pytest.importorskip("pandas")

    catalog_data = {
        "postgres_jdbc": {
            "engine": "pandas",
            "format": "postgres",
            "connector": "jdbc",
            "storage_path": "core.finance_ledger",
            "options": {
                "url": "jdbc:postgresql://localhost:5432/finance",
                "driver": "org.postgresql.Driver",
            },
        }
    }

    catalog_dir = _setup_mock_project_environment(tmp_path, catalog_data)
    catalog = DataCatalog(catalog_path=catalog_dir)
    mock_jaydebeapi = MagicMock()

    with patch.dict("sys.modules", {"jaydebeapi": mock_jaydebeapi}), patch("pandas.read_sql_query") as mock_sql_query:
        mock_sql_query.return_value = pd.DataFrame()

        catalog.load("postgres_jdbc")

        mock_jaydebeapi.connect.assert_called_once_with(
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/finance",
            [],
            None,
        )


def test_pandas_odbc_connector_factory(tmp_path: Path) -> None:
    """Asserts that relational engine formats map via explicit ODBC descriptors."""
    pd = pytest.importorskip("pandas")

    catalog_data = {
        "mysql_odbc": {
            "engine": "pandas",
            "format": "mysql",
            "connector": "odbc",
            "storage_path": "warehouse.sales_fact",
            "options": {
                "dsn": "MySQL_Prod",
                "uid": "admin",
                "pwd": "secret_password",
            },
        }
    }

    catalog_dir = _setup_mock_project_environment(tmp_path, catalog_data)
    catalog = DataCatalog(catalog_path=catalog_dir)
    mock_pyodbc = MagicMock()

    with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}), patch("pandas.read_sql_query") as mock_sql_query:
        mock_sql_query.return_value = pd.DataFrame()

        catalog.load("mysql_odbc")

        # Expectations synchronized with alphabetically sorted DSN properties
        mock_pyodbc.connect.assert_called_once_with("DSN=MySQL_Prod;pwd=secret_password;uid=admin")


def test_pandas_databricks_native_connector(tmp_path: Path) -> None:
    """Asserts that Databricks native connectors isolate warehouse options."""
    pd = pytest.importorskip("pandas")

    catalog_data = {
        "dbx_native": {
            "engine": "pandas",
            "format": "databricks",
            "connector": "native",
            "storage_path": "catalog.schema.table",
            "options": {
                "server_hostname": "adb-test.net",
                "http_path": "/sql/1.0/warehouses/abc",
                "access_token": "dapi_token",
            },
        }
    }

    catalog_dir = _setup_mock_project_environment(tmp_path, catalog_data)
    catalog = DataCatalog(catalog_path=catalog_dir)

    mock_sql = MagicMock()
    mock_connect = MagicMock()
    mock_sql.connect = mock_connect

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall_as_pandas.return_value = pd.DataFrame()

    # Explicitly bind the sql attribute onto the parent mock to close the trap
    mock_databricks = MagicMock()
    mock_databricks.sql = mock_sql

    mock_modules = {
        "databricks": mock_databricks,
        "databricks.sql": mock_sql,
    }

    with patch.dict("sys.modules", mock_modules):
        catalog.load("dbx_native")

        mock_connect.assert_called_once_with(
            server_hostname="adb-test.net",
            http_path="/sql/1.0/warehouses/abc",
            access_token="dapi_token",
        )


def test_spark_unity_catalog_three_level_namespace(tmp_path: Path) -> None:
    """Verifies that Spark intercepts namespaces utilizing session.table()."""
    catalog_data = {
        "uc_table": {
            "engine": "spark",
            "format": "databricks",
            "storage_path": "main_catalog.gold_schema.f1_drivers",
        }
    }

    catalog_dir = _setup_mock_project_environment(tmp_path, catalog_data)
    catalog = DataCatalog(catalog_path=catalog_dir)

    mock_spark_session = MagicMock()
    mock_spark_session._sc = MagicMock()

    with (
        patch("pyspark.sql.SparkSession.getActiveSession") as mock_get_active,
        patch("pyspark.sql.readwriter.DataFrameReader.format") as mock_format,
    ):
        mock_get_active.return_value = mock_spark_session

        catalog.load("uc_table", spark=mock_spark_session)

        mock_spark_session.table.assert_called_once_with("main_catalog.gold_schema.f1_drivers")
        mock_format.assert_not_called()
