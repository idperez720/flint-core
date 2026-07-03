"""Metadata-driven, environment-agnostic data loading and saving utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

from flint_core.core.base import EngineRegistry
from flint_core.core.catalog.models import DatasetConfiguration


def _resolve_path(raw_path: str, project_root: Path) -> str:
    """Resolves target paths, safeguarding cloud URIs from local expansion."""
    parsed = urlparse(raw_path)
    if parsed.scheme in (
        "s3",
        "s3a",
        "s3n",
        "gs",
        "gcs",
        "abfss",
        "az",
        "wasb",
        "wasbs",
    ):
        return raw_path

    file_path = Path(raw_path)
    if not file_path.is_absolute():
        return str((project_root / file_path).resolve())
    return str(file_path.resolve())


class DataLoader:
    """Handles dynamic loading of data elements by delegating to engines."""

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataLoader with a specific DataCatalog reference."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def load(
        self,
        dataset_name: str,
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
        version: Optional[Union[int, str]] = None,
        as_of: Optional[str] = None,
    ) -> Any:
        """Loads a dataset from storage utilizing dynamic engine dispatching.

        Args:
            dataset_name: Name of the target dataset configuration key.
            spark: Optional active distributed SparkSession manager reference.
            options: Optional runtime reading options overrides.
            version: Optional target version index for Lakehouse time travel.
            as_of: Optional chronological timestamp for historical snapshots.

        Returns:
            Any: The loaded DataFrame structure.
        """
        dataset: DatasetConfiguration = self.catalog.get_dataset(dataset_name)
        resolved_path = _resolve_path(dataset.storage_path, self.catalog.project_root)

        raw_catalog_opts = dataset.metadata.get("options", {})
        catalog_options = raw_catalog_opts if isinstance(raw_catalog_opts, dict) else {}
        runtime_options = options if options is not None else {}

        combined_options = {
            **catalog_options,
            **runtime_options,
        }

        # Inject unified time-travel parameters for Lakehouse layers
        if version is not None:
            combined_options["versionAsOf"] = version
        if as_of is not None:
            combined_options["timestampAsOf"] = as_of

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = combined_options

        engine = EngineRegistry.get_engine(dataset.engine)
        return engine.load(
            path=resolved_path,
            data_format=dataset.format,
            columns=dataset.columns,
            metadata=combined_metadata,
            spark=spark,
        )


class DataSaver:
    """Handles dynamic saving of data elements by delegating to engines."""

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataSaver with a specific DataCatalog reference."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def save(
        self,
        df: Any,
        dataset_name: str,
        mode: str = "error",
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Saves a dataframe utilizing dynamic engine dispatching."""
        dataset = self.catalog.get_dataset(dataset_name)
        resolved_path = _resolve_path(dataset.storage_path, self.catalog.project_root)

        raw_catalog_opts = dataset.metadata.get("options", {})
        catalog_options = raw_catalog_opts if isinstance(raw_catalog_opts, dict) else {}
        runtime_options = options if options is not None else {}

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = {
            **catalog_options,
            **runtime_options,
        }

        engine = EngineRegistry.get_engine(dataset.engine)
        engine.save(
            df=df,
            path=resolved_path,
            data_format=dataset.format,
            columns=dataset.columns,
            mode=mode,
            metadata=combined_metadata,
            spark=spark,
        )
