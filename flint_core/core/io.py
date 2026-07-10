"""Metadata-driven, environment-agnostic data loading and saving utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

from flint_core.core.base import EngineRegistry
from flint_core.core.catalog.models import DatasetConfiguration


def _resolve_path(raw_path: str, project_root: Path) -> str:
    """Resolves deployment path targets protecting multi-cloud storage namespaces.

    Args:
        raw_path: Unprocessed declarative pathway string identifier token.
        project_root: Local fallback anchor execution path directory reference.

    Returns:
        str: Absolute system path or intact multi-cloud URI protocol string.
    """
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

    if "/" not in raw_path and "\\" not in raw_path:
        return raw_path

    file_path = Path(raw_path)
    if not file_path.is_absolute():
        return str((project_root / file_path).resolve())
    return str(file_path.resolve())


class DataLoader:
    """Unified loading execution controller directing polymorphic data input streams.

    Acts as an inversion boundary consuming configurations maps and dispatching
    evaluation workflows to registered processing drivers.
    """

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataLoader wrapping an active data catalog context."""
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
        """Triggers declarative catalog dataset ingestion parsing requirements.

        Args:
            dataset_name: Core metadata token catalog identifier key.
            spark: Optional active distributed SparkSession engine manager.
            options: Runtime dictionary configuration reading overrides.
            version: Target version snapshot tracker for time travel.
            as_of: Chronological timestamp target snapshot constraint.

        Returns:
            Any: Target computational dataframe instance.
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

        if version is not None:
            combined_options["versionAsOf"] = version
        if as_of is not None:
            combined_options["timestampAsOf"] = as_of

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = combined_options
        combined_metadata["connector"] = dataset.connector

        engine = EngineRegistry.get_engine(dataset.engine)
        return engine.load(
            path=resolved_path,
            data_format=dataset.format,
            columns=dataset.columns,
            metadata=combined_metadata,
            spark=spark,
        )


class DataSaver:
    """Unified persistence manager orchestrating dynamic computational outputs.

    Validates schema structures constraints prior to execution and routes
    payload transformations to downstream storage engine layers.
    """

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataSaver wrapping an active data catalog context."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def save(
        self,
        df: Any,
        dataset_name: str,
        mode: str = "error",
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
        replace_where: Optional[str] = None,
    ) -> None:
        """Saves a polymorphic dataframe executing engine write procedures.

        Args:
            df: Target polymorphic DataFrame instance to persist.
            dataset_name: Core metadata token destination lookup key.
            mode: Operational write behavior strategy rule instructions.
            spark: Optional active distributed SparkSession engine context.
            options: Runtime dictionary configuration writing overrides mapping.
            replace_where: Optional condition for replacing existing data.
        """
        if replace_where and mode != "overwrite":
            raise ValueError("The 'replace_where' parameter can only be utilized when mode='overwrite'.")
        
        dataset = self.catalog.get_dataset(dataset_name)

        # CRITICAL VALIDATION ENFORCEMENT: Guard schema contract boundaries
        dataset.validate_schema(df)

        resolved_path = _resolve_path(dataset.storage_path, self.catalog.project_root)

        raw_catalog_opts = dataset.metadata.get("options", {})
        catalog_options = raw_catalog_opts if isinstance(raw_catalog_opts, dict) else {}
        runtime_options = options if options is not None else {}

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = {
            **catalog_options,
            **runtime_options,
        }
        combined_metadata["connector"] = dataset.connector
        combined_metadata["replace_where"] = replace_where

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
