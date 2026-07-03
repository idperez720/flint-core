"""Unit tests for flint-core Lakehouse time-travel and format handling layer."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from flint_core.core.catalog import DataCatalog


def test_lakehouse_time_travel_options_propagation(tmp_path: Path) -> None:
    """Asserts that version parameters translate correctly into engine choices."""
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-lakehouse"\n')

    lake_yaml = {
        "gold_transactions": {
            "engine": "pandas",
            "format": "delta",
            "storage_path": "data/lakehouse/gold_tx",
        }
    }

    with open(catalog_dir / "lake.yaml", "w", encoding="utf-8") as f:
        yaml.dump(lake_yaml, f)

    catalog = DataCatalog(catalog_path=catalog_dir)

    mock_deltalake = MagicMock()
    with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
        mock_table = MagicMock()
        mock_table.to_pandas.return_value = "mocked_df"
        mock_deltalake.DeltaTable.return_value = mock_table

        res = catalog.load("gold_transactions", version=12)

        assert res == "mocked_df"
        mock_deltalake.DeltaTable.assert_called_once_with(
            str((tmp_path / "data/lakehouse/gold_tx").resolve()),
            version=12,
            datetime=None,
        )


def test_lakehouse_time_travel_as_of_propagation(tmp_path: Path) -> None:
    """Asserts that timestamp parameters translate correctly into engine choices."""
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-lakehouse-ts"\n')

    lake_yaml = {
        "gold_ts": {
            "engine": "pandas",
            "format": "delta",
            "storage_path": "data/lakehouse/gold_ts",
        }
    }

    with open(catalog_dir / "lake.yaml", "w", encoding="utf-8") as f:
        yaml.dump(lake_yaml, f)

    catalog = DataCatalog(catalog_path=catalog_dir)

    mock_deltalake = MagicMock()
    with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
        mock_table = MagicMock()
        mock_table.to_pandas.return_value = "mocked_df"
        mock_deltalake.DeltaTable.return_value = mock_table

        res = catalog.load("gold_ts", as_of="2026-06-27T00:00:00Z")

        assert res == "mocked_df"
        mock_deltalake.DeltaTable.assert_called_once_with(
            str((tmp_path / "data/lakehouse/gold_ts").resolve()),
            version=None,
            datetime="2026-06-27T00:00:00Z",
        )
