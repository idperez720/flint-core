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
from flint_core.core.catalog.models import ColumnDefinition, DatasetConfiguration
from flint_core.core.exceptions import CatalogParseError

logger = logging.getLogger(__name__)


class DataCatalog:
    """Enterprise declarative file parsing synchronizer control center."""

    _executor: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix="FlintCatalogWorker")

    def __init__(self, catalog_path: Optional[Union[str, Path]] = None) -> None:
        self._lock: RLock = RLock()
        self.project_root: Path = Path()
        self._datasets: Dict[str, DatasetConfiguration] = {}

        if catalog_path is None:
            resolved_path = self._discover_catalog_path()
        else:
            resolved_path = Path(catalog_path).resolve()
            self._find_project_root_from_path(resolved_path)
            if not resolved_path.exists():
                raise FileNotFoundError(f"Source path missing: {resolved_path}")

        self.reload_catalog(resolved_path)

    def __getitem__(self, dataset_name: str) -> DatasetConfiguration:
        return self.get_dataset(dataset_name)

    def __contains__(self, dataset_name: str) -> bool:
        with self._lock:
            return dataset_name in self._datasets

    @property
    def dataset_names(self) -> List[str]:
        with self._lock:
            return list(self._datasets.keys())

    def get_dataset(self, dataset_name: str) -> DatasetConfiguration:
        with self._lock:
            if dataset_name not in self._datasets:
                raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")
            return self._datasets[dataset_name]

    def get_spark_configuration(self) -> Dict[str, Any]:
        """Parses and extracts global parameters inside conf/spark.yml."""
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
        """Loads a dataset from storage utilizing dynamic engine dispatching."""
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
        with self._lock:
            self._datasets.clear()
            self._load_catalog_sources(path)
            self._bind_dynamic_descriptors()

    async def reload_catalog_async(self, path: Path) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.reload_catalog, path)

    def _bind_dynamic_descriptors(self) -> None:
        for name in self._datasets:
            if not hasattr(self, name):
                setattr(self.__class__, name, DatasetDescriptor(name))

    def _discover_catalog_path(self) -> Path:
        current_dir = Path.cwd().resolve()
        for parent in [current_dir] + list(current_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return parent / "conf" / "catalog"
        raise FileNotFoundError("Could not locate root pyproject.toml environmental anchor.")

    def _find_project_root_from_path(self, target_path: Path) -> None:
        for parent in [target_path] + list(target_path.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return
        self.project_root = target_path

    def _load_catalog_sources(self, path: Path) -> None:
        if path.is_file() and path.suffix in (".yml", ".yaml"):
            self._parse_file(path)
            return
        for file_path in path.rglob("*"):
            if file_path.is_file() and file_path.suffix in (".yml", ".yaml"):
                self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> None:
        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise CatalogParseError(f"Syntax error in '{file_path.name}': {e}") from e

        if content and isinstance(content, dict):
            for dataset_name, dataset_body in content.items():
                if not isinstance(dataset_body, dict):
                    continue
                engine = dataset_body.get("engine")
                data_format = dataset_body.get("format")
                storage_path = dataset_body.get("storage_path")

                if not engine or not data_format or not storage_path:
                    raise KeyError("Metadata must track engine, format and path.")

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
                    k: v for k, v in dataset_body.items() if k not in ("columns", "engine", "format", "storage_path")
                }
                with self._lock:
                    self._datasets[dataset_name] = DatasetConfiguration(
                        name=dataset_name,
                        engine=engine,
                        data_format=data_format,
                        storage_path=storage_path,
                        columns=column_definitions,
                        metadata=metadata_payload,
                        catalog_ref=self,
                    )
