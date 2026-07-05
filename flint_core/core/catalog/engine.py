"""This module implements the concurrent orchestrator managing data catalogs."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Union

import yaml

from flint_core.core.catalog.descriptors import DatasetDescriptor
from flint_core.core.catalog.models import (
    ColumnDefinition,
    DatasetConfiguration,
)
from flint_core.core.exceptions import CatalogParseError

logger = logging.getLogger(__name__)


class DataCatalog:
    """Enterprise declarative file-parsing catalog configuration management system.

    Coordinates concurrent access to distributed metadata descriptor layers,
    injecting infrastructure properties and resolving dataset configurations.
    """

    _executor: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix="FlintCatalogWorker")

    def __init__(self, catalog_path: Optional[Union[str, Path]] = None) -> None:
        """Initializes the synchronized DataCatalog mapping configuration schemas."""
        self._lock: RLock = RLock()
        self.project_root: Path = Path()
        self._datasets: Dict[str, DatasetConfiguration] = {}

        if catalog_path is None:
            resolved_path = self._discover_catalog_path()
        else:
            resolved_path = Path(catalog_path).resolve()
            self._find_project_root_from_path(resolved_path)
            if not resolved_path.exists():
                raise FileNotFoundError(f"Target file path missing: {resolved_path}")

        self.reload_catalog(resolved_path)

    def __getitem__(self, dataset_name: str) -> DatasetConfiguration:
        """Allows direct element lookup leveraging native catalog key accessors."""
        return self.get_dataset(dataset_name)

    def __contains__(self, dataset_name: str) -> bool:
        """Determines if a dataset identifier exists inside active tables."""
        with self._lock:
            return dataset_name in self._datasets

    @property
    def dataset_names(self) -> List[str]:
        """Retrieves all registered dataset keys from synchronized registers."""
        with self._lock:
            return list(self._datasets.keys())

    def get_dataset(self, dataset_name: str) -> DatasetConfiguration:
        """Fetches a designated isolated DatasetConfiguration entity model.

        Args:
            dataset_name: Target logical catalog identifier lookup key.

        Returns:
            DatasetConfiguration: Memory-isolated layout data model.
        """
        with self._lock:
            if dataset_name not in self._datasets:
                raise KeyError(f"Dataset '{dataset_name}' missing from active catalog.")
            return self._datasets[dataset_name]

    def get_spark_configuration(self) -> Dict[str, Any]:
        """Parses and extracts global parameters inside conf/spark.yml conventions.

        Returns:
            Dict[str, Any]: Key-value session operational configuration mappings.
        """
        spark_path = self.project_root / "conf" / "spark.yml"
        if not spark_path.exists():
            spark_path = self.project_root / "conf" / "spark.yaml"

        if not spark_path.exists():
            logger.debug("No global spark convention file found at %s", spark_path)
            return {}

        try:
            with open(spark_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.error("Failed to parse Spark conventions at %s: %s", spark_path, e)
            raise CatalogParseError(f"Syntax anomaly in '{spark_path.name}': {e}") from e

        if content and isinstance(content, dict):
            return {str(k): v for k, v in content.items()}
        return {}

    def load(
        self,
        dataset_name: str,
        spark: Optional[Any] = None,
        version: Optional[Union[int, str]] = None,
        as_of: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Loads a target dataset from storage by delegating to specialized engines.

        Args:
            dataset_name: Target logical configuration registry key identifier.
            spark: Optional active distributed SparkSession manager reference.
            version: Optional target version snapshot index for time travel.
            as_of: Optional chronological timestamp target for snapshots.
            options: Optional override dictionary parameters mapping.

        Returns:
            Any: Polmorphic evaluation DataFrame data structure matrix.
        """
        from flint_core.core.io import DataLoader

        with self._lock:
            loader = DataLoader(catalog=self)
            return loader.load(
                dataset_name,
                spark=spark,
                options=options,
                version=version,
                as_of=as_of,
            )

    def save(
        self,
        df: Any,
        dataset_name: str,
        mode: str = "error",
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Saves a dataset executing dynamic engine-specific write procedures.

        Args:
            df: Polymorphic source computational DataFrame instance.
            dataset_name: Target logical configuration destination key identifier.
            mode: Save behavior descriptor mode ('overwrite', 'append', etc.).
            spark: Optional active distributed SparkSession manager reference.
            options: Optional override options dictionary mapping payload.
        """
        from flint_core.core.io import DataSaver

        with self._lock:
            saver = DataSaver(catalog=self)
            saver.save(
                df=df,
                dataset_name=dataset_name,
                mode=mode,
                spark=spark,
                options=options,
            )

    def reload_catalog(self, path: Path) -> None:
        """Flushes registers rebuilding the complete metadata catalog space.

        Args:
            path: Target Directory directory mapping localized YAML trees.
        """
        with self._lock:
            self._datasets.clear()
            self._load_catalog_sources(path)
            self._bind_dynamic_descriptors()

    async def reload_catalog_async(self, path: Path) -> None:
        """Asynchronously triggers catalog reloading utilizing workers threads."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.reload_catalog, path)

    def _bind_dynamic_descriptors(self) -> None:
        """Weaves runtime programmatic descriptive fluent property dot accessors."""
        for name in self._datasets:
            if not hasattr(self, name):
                setattr(self.__class__, name, DatasetDescriptor(name))

    def _discover_catalog_path(self) -> Path:
        """Executes recursive parent directories tree-walking seeking project anchors."""
        current_dir = Path.cwd().resolve()
        for parent in [current_dir] + list(current_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return parent / "conf" / "catalog"
        raise FileNotFoundError("Target repository anchor file (pyproject.toml) missing.")

    def _find_project_root_from_path(self, path: Path) -> None:
        """Triggers project root resolution tracking derived from paths arguments."""
        start_dir = path if path.is_dir() else path.parent
        for parent in [start_dir] + list(start_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return
        self.project_root = start_dir

    def _load_catalog_sources(self, path: Path) -> None:
        """Traverses target paths scanning valid structural YAML catalog files."""
        if path.is_file() and path.suffix in (".yml", ".yaml"):
            self._parse_file(path)
            return
        for file_path in path.rglob("*"):
            if file_path.is_file() and file_path.suffix in (".yml", ".yaml"):
                self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> None:
        """Parses individual YAML target files compiling configurations maps."""
        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise CatalogParseError(f"Syntax error inside target file '{file_path.name}': {e}") from e

        if content and isinstance(content, dict):
            for dataset_name, dataset_body in content.items():
                if not isinstance(dataset_body, dict):
                    continue
                engine = dataset_body.get("engine")
                data_format = dataset_body.get("format")
                storage_path = dataset_body.get("storage_path")
                connector = dataset_body.get("connector")

                if not engine or not data_format or not storage_path:
                    raise KeyError("Mandatory tags engine, format and path must be tracked.")

                raw_columns = dataset_body.get("columns", []) or []
                column_definitions: List[ColumnDefinition] = []
                for col in raw_columns:
                    if isinstance(col, dict) and "name" in col:
                        column_definitions.append(
                            ColumnDefinition(
                                name=col["name"],
                                data_type=col.get("type"),
                                description=col.get("description"),
                                column_format=col.get("format"),
                                timezone=col.get("timezone"),
                            )
                        )
                metadata_payload = {
                    k: v
                    for k, v in dataset_body.items()
                    if k
                    not in (
                        "columns",
                        "engine",
                        "format",
                        "storage_path",
                        "connector",
                    )
                }
                with self._lock:
                    self._datasets[dataset_name] = DatasetConfiguration(
                        name=dataset_name,
                        engine=engine,
                        data_format=data_format,
                        storage_path=storage_path,
                        columns=column_definitions,
                        metadata=metadata_payload,
                        connector=connector,
                        catalog_ref=self,
                    )
