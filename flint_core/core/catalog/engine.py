"""This module implements the concurrent orchestrator managing multi-environment data catalogs."""

from __future__ import annotations

import asyncio
import logging
import os
import re
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

    Coordinates concurrent access to distributed metadata descriptor layers, resolves active operational profiles via
    environmental Inversion of Control (IoC), and compiles runtime token substitution interpolation patterns.
    """

    _executor: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix="FlintCatalogWorker")

    def __init__(self, catalog_path: Optional[Union[str, Path]] = None, env: Optional[str] = None) -> None:
        """Initializes the synchronized DataCatalog mapping multi-environment configuration schemas."""
        self._lock: RLock = RLock()
        self.project_root: Path = Path()
        self._datasets: Dict[str, DatasetConfiguration] = {}

        # 1. Hierarchical resolution of the active operational environment profile
        self.active_env: str = env or os.getenv("FLINT_ENV", "dev")

        # 2. Discover and establish baseline repository context directory paths
        if catalog_path is None:
            resolved_path = self._discover_catalog_path()
        else:
            resolved_path = Path(catalog_path).resolve()
            self._find_project_root_from_path(resolved_path)
            if not resolved_path.exists():
                raise FileNotFoundError(f"Target file path missing: {resolved_path}")

        # 3. Cache only the localized variables corresponding to the active runtime profile
        self.env_variables: Dict[str, Any] = self._load_environment_variables()

        # 4. Trigger asynchronous-ready catalog source building
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
        """Fetches a designated isolated DatasetConfiguration entity model."""
        with self._lock:
            if dataset_name not in self._datasets:
                raise KeyError(f"Dataset '{dataset_name}' missing from active catalog.")
            return self._datasets[dataset_name]

    def get_spark_configuration(self) -> Dict[str, Any]:
        """Parses and extracts global and environment-specific parameters.

        Loads baseline configurations from conf/spark.yml and merges environment-specific overrides from
        conf/envs/{env}/spark.yml. Environmental settings take precedence.

        Returns:
            Dict[str, Any]: Merged session operational configuration mappings for the active environment.
        """
        combined_config: Dict[str, Any] = {}

        # Step 1: Load baseline global configurations if present (Preserves legacy backward compatibility)
        global_path = self.project_root / "conf" / "spark.yml"
        if not global_path.exists():
            global_path = self.project_root / "conf" / "spark.yaml"

        if global_path.exists():
            try:
                with open(global_path, "r", encoding="utf-8") as stream:
                    global_content = yaml.safe_load(stream)
                    if global_content and isinstance(global_content, dict):
                        combined_config.update({str(k): v for k, v in global_content.items()})
            except yaml.YAMLError as e:
                logger.error("Failed to parse global Spark conventions at %s: %s", global_path, e)
                raise CatalogParseError(f"Syntax anomaly in global '{global_path.name}': {e}") from e

        # Step 2: Load environment-specific overrides if present (Phase 3.5 Multi-environment support)
        env_path = self.project_root / "conf" / "envs" / self.active_env / "spark.yml"
        if not env_path.exists():
            env_path = self.project_root / "conf" / "envs" / self.active_env / "spark.yaml"

        if env_path.exists():
            try:
                with open(env_path, "r", encoding="utf-8") as stream:
                    env_content = yaml.safe_load(stream)
                    if env_content and isinstance(env_content, dict):
                        # Environmental properties safely overwrite global baselines
                        combined_config.update({str(k): v for k, v in env_content.items()})
            except yaml.YAMLError as e:
                logger.error("Failed to parse isolated Spark conventions at %s: %s", env_path, e)
                raise CatalogParseError(f"Syntax anomaly in isolated '{env_path.parent.name}/spark.yml': {e}") from e

        return combined_config

    def load(
        self,
        dataset_name: str,
        spark: Optional[Any] = None,
        version: Optional[Union[int, str]] = None,
        as_of: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Loads a target dataset from storage by delegating to specialized engines."""
        from flint_core.core.io import DataLoader

        with self._lock:
            loader = DataLoader(catalog=self)
            return loader.load(dataset_name, spark=spark, options=options, version=version, as_of=as_of)

    def save(
        self,
        df: Any,
        dataset_name: str,
        mode: str = "error",
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Saves a dataset executing dynamic engine-specific write procedures."""
        from flint_core.core.io import DataSaver

        with self._lock:
            saver = DataSaver(catalog=self)
            saver.save(df=df, dataset_name=dataset_name, mode=mode, spark=spark, options=options)

    def reload_catalog(self, path: Path) -> None:
        """Flushes registers rebuilding the complete metadata catalog space."""
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

    def _load_environment_variables(self) -> Dict[str, Any]:
        """Reads the isolated variables configuration file inside the active environment folder."""
        var_path = self.project_root / "conf" / "envs" / self.active_env / "variables.yml"
        if not var_path.exists():
            var_path = self.project_root / "conf" / "envs" / self.active_env / "variables.yaml"

        if not var_path.exists():
            logger.debug("Isolated environment data missing at %s. Proceeding with OS variables.", var_path)
            return {}

        try:
            with open(var_path, "r", encoding="utf-8") as stream:
                env_vars = yaml.safe_load(stream) or {}
        except yaml.YAMLError as e:
            logger.error("Failed to compile isolated contextual environmental fields: %s", e)
            raise CatalogParseError(
                f"Syntax anomaly within isolated tokens manifest for '{self.active_env}': {e}"
            ) from e

        if not isinstance(env_vars, dict):
            return {}

        return env_vars

    def _interpolate_payload(self, data: Any) -> Any:
        """Recursively parses and resolves string token placeholders using environment metadata."""
        if isinstance(data, str):
            pattern = re.compile(r"\$\{(\w+)\}")

            def replacer(match: re.Match[str]) -> str:
                var_name = match.group(1)
                return str(self.env_variables.get(var_name, os.getenv(var_name, match.group(0))))

            return pattern.sub(replacer, data)
        elif isinstance(data, dict):
            return {k: self._interpolate_payload(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._interpolate_payload(item) for item in data]
        return data

    def _load_catalog_sources(self, path: Path) -> None:
        """Traverses target paths scanning valid structural YAML catalog files."""
        if path.is_file() and path.suffix in (".yml", ".yaml"):
            self._parse_file(path)
            return
        for file_path in path.rglob("*"):
            if file_path.is_file() and file_path.suffix in (".yml", ".yaml"):
                self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> None:
        """Parses individual YAML target files compiling and interpolating configuration mappings."""
        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise CatalogParseError(f"Syntax error inside target file '{file_path.name}': {e}") from e

        if content and isinstance(content, dict):
            # Phase 3.5: Execute recursive interpolation across the compiled metadata pool
            content = self._interpolate_payload(content)

            for dataset_name, dataset_body in content.items():
                if not isinstance(dataset_body, dict) or dataset_name.startswith("."):
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
                    if k not in ("columns", "engine", "format", "storage_path", "connector")
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
