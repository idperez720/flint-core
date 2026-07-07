"""This module implements transactional project scaffolding engines for flint."""

import logging
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from flint_core.core.exceptions import ProjectInitializationError

logger = logging.getLogger(__name__)

LAYOUT_SUBDIRS: Dict[str, List[str]] = {
    "default": [],
    "medallion": ["bronze", "silver", "gold"],
    "kimball": ["staging", "dimensions", "facts"],
    "inmon": ["staging", "corporate_edw", "data_marts"],
    "datavault": ["raw_vault", "business_vault", "info_marts"],
}


class ScaffoldContext:
    """High-performance container tracking pipeline execution parameters."""

    __slots__ = (
        "root_path",
        "name",
        "version",
        "description",
        "author",
        "metadata",
        "created_paths",
    )

    def __init__(
        self,
        root_path: Path,
        name: str,
        version: str,
        description: str,
        author: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.root_path: Path = root_path.resolve()
        self.name: str = name
        self.version: str = version
        self.description: str = description
        self.author: str = author
        self.metadata: Dict[str, Any] = metadata if metadata is not None else {}
        self.created_paths: List[Path] = []


@runtime_checkable
class ScaffoldStep(Protocol):
    """Structural protocol governing atomic project initialization stages."""

    @property
    def name(self) -> str: ...

    def execute(self, context: ScaffoldContext) -> None: ...

    def rollback(self, context: ScaffoldContext) -> None: ...


class DirectoryStructureStep:
    """Core stage orchestrating allocation of physical folders layouts."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Directory Structure Layout Scaffolding"

    def execute(self, context: ScaffoldContext) -> None:
        pattern = context.metadata.get("pattern", "default")
        envs = context.metadata.get("envs", ["dev", "qa", "prod"])
        domains = context.metadata.get("domains", [])

        target_folders = [
            context.root_path / "src" / "notebooks",
            context.root_path / "data",
            context.root_path / "conf" / "catalog",
        ]

        for env in envs:
            target_folders.append(context.root_path / "conf" / "envs" / env)

        if pattern in ("datamart", "datamesh"):
            for domain in domains:
                target_folders.append(context.root_path / "conf" / "catalog" / domain)
        else:
            subdirs = LAYOUT_SUBDIRS.get(pattern, [])
            for subdir in subdirs:
                target_folders.append(context.root_path / "conf" / "catalog" / subdir)

        for folder in target_folders:
            parts_to_create = []
            current = folder
            while current != context.root_path and current != current.parent:
                if not current.exists():
                    parts_to_create.append(current)
                    current = current.parent
                else:
                    break
            for part in reversed(parts_to_create):
                part.mkdir(exist_ok=True)
                context.created_paths.append(part)

    def rollback(self, context: ScaffoldContext) -> None:
        for item in reversed(context.created_paths):
            if item.is_dir() and item.exists() and not any(item.iterdir()):
                item.rmdir()


class PyProjectTomlStep:
    """Core stage orchestrating safe generation of project environment anchors."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Root Configuration Manifest Generation"

    def execute(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError("A configuration manifest file already exists at target path.")
        toml_content = textwrap.dedent(f"""\
            [project]
            name = "{context.name}"
            version = "{context.version}"
            description = "{context.description}"
            authors = [
                {{name = "{context.author}"}}
            ]
            requires-python = ">=3.11,<4.0.0"
        """)
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(toml_content)
        context.created_paths.append(toml_path)

    def rollback(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.is_file() and toml_path.exists():
            toml_path.unlink()


class SampleDataStep:
    """Optional baseline step dropping an operational seed table asset."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Boilerplate Seed Sample Physical Data Insertion"

    def execute(self, context: ScaffoldContext) -> None:
        csv_path = context.root_path / "data" / "sample_table.csv"
        csv_content = textwrap.dedent("""\
            id,name
            1,Alice
            2,Bob
        """)
        with open(csv_path, "w", encoding="utf-8") as csv_file:
            csv_file.write(csv_content)
        context.created_paths.append(csv_path)

    def rollback(self, context: ScaffoldContext) -> None:
        csv_path = context.root_path / "data" / "sample_table.csv"
        if csv_path.is_file() and csv_path.exists():
            csv_path.unlink()


class SampleCatalogStep:
    """Core step linking the seed boilerplate onto decentral catalog structures."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Seed Declarative Catalog Configuration Generation"

    def _write_file(self, path: Path, content: str, context: ScaffoldContext) -> None:
        with open(path, "w", encoding="utf-8") as file:
            file.write(textwrap.dedent(content))
        context.created_paths.append(path)

    def execute(self, context: ScaffoldContext) -> None:
        pattern = context.metadata.get("pattern", "default")
        domains = context.metadata.get("domains", [])

        generic_content = """\
            sample_table:
              description: 'Boilerplate example dataset created by flint'
              format: 'csv'
              engine: 'pandas'
              storage_path: 'data/sample_table.csv'
              columns:
                - name: 'id'
                  type: 'integer'
                - name: 'name'
                  type: 'string'
        """

        if pattern == "default":
            path = context.root_path / "conf" / "catalog" / "datasets.yaml"
            self._write_file(path, generic_content, context)
        elif pattern in ("datamart", "datamesh"):
            contract_content = """\
                # Domain specific data contract layout parameters
                domain_dataset:
                  description: 'Domain template dataset framework reference'
                  format: 'parquet'
                  engine: 'pandas'
                  storage_path: 'data/domain_data.parquet'
            """
            for domain in domains:
                path = context.root_path / "conf" / "catalog" / domain / "sample_contracts.yaml"
                self._write_file(path, contract_content, context)
        else:
            subdirs = LAYOUT_SUBDIRS.get(pattern, [])
            for subdir in subdirs:
                path = context.root_path / "conf" / "catalog" / subdir / "sample_dataset.yaml"
                self._write_file(path, generic_content, context)

    def rollback(self, context: ScaffoldContext) -> None:
        pattern = context.metadata.get("pattern", "default")
        domains = context.metadata.get("domains", [])

        paths_to_delete = []
        if pattern == "default":
            paths_to_delete.append(context.root_path / "conf" / "catalog" / "datasets.yaml")
        elif pattern in ("datamart", "datamesh"):
            for domain in domains:
                paths_to_delete.append(context.root_path / "conf" / "catalog" / domain / "sample_contracts.yaml")
        else:
            subdirs = LAYOUT_SUBDIRS.get(pattern, [])
            for subdir in subdirs:
                paths_to_delete.append(context.root_path / "conf" / "catalog" / subdir / "sample_dataset.yaml")

        for path in paths_to_delete:
            if path.is_file() and path.exists():
                path.unlink()


class SampleSparkConfigStep:
    """Core step generating a baseline template for global cloud runtimes."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Seed Spark Configuration Template Generation"

    def execute(self, context: ScaffoldContext) -> None:
        spark_path = context.root_path / "conf" / "spark.yml"
        spark_content = textwrap.dedent("""\
            # Global Spark configurations managed by convention via flint-core
            spark.sql.shuffle.partitions: "2"
            spark.default.parallelism: "2"
            spark.sql.execution.arrow.pyspark.enabled: "true"
        """)
        with open(spark_path, "w", encoding="utf-8") as file:
            file.write(spark_content)
        context.created_paths.append(spark_path)

    def rollback(self, context: ScaffoldContext) -> None:
        spark_path = context.root_path / "conf" / "spark.yml"
        if spark_path.is_file() and spark_path.exists():
            spark_path.unlink()


class IsolatedEnvironmentTemplatesStep:
    """Generates isolated environment configuration templates automatically."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Isolated Environment Sandbox Configuration Generation"

    def execute(self, context: ScaffoldContext) -> None:
        envs = context.metadata.get("envs", ["dev", "qa", "prod"])
        for env in envs:
            var_path = context.root_path / "conf" / "envs" / env / "variables.yml"
            spark_path = context.root_path / "conf" / "envs" / env / "spark.yml"

            var_content = textwrap.dedent(f"""\
                # Isolated variables environment configurations for: {env}
                environment_tier: "{env}"
                datalake_bucket: "my-flint-datalake-sandbox-{env}"
            """)
            spark_content = textwrap.dedent(f"""\
                # Dynamic context runtime session overrides for tier: {env}
                spark.sql.shuffle.partitions: "4"
            """)

            with open(var_path, "w", encoding="utf-8") as f:
                f.write(var_content)
            context.created_paths.append(var_path)

            with open(spark_path, "w", encoding="utf-8") as f:
                f.write(spark_content)
            context.created_paths.append(spark_path)

    def rollback(self, context: ScaffoldContext) -> None:
        envs = context.metadata.get("envs", ["dev", "qa", "prod"])
        for env in envs:
            var_path = context.root_path / "conf" / "envs" / env / "variables.yml"
            spark_path = context.root_path / "conf" / "envs" / env / "spark.yml"
            if var_path.is_file() and var_path.exists():
                var_path.unlink()
            if spark_path.is_file() and spark_path.exists():
                spark_path.unlink()


class ProjectInitializer:
    """Orchestrates modular pipeline stages under rigid transaction blocks."""

    _pipeline: List[ScaffoldStep] = [
        DirectoryStructureStep(),
        PyProjectTomlStep(),
        SampleDataStep(),
        SampleCatalogStep(),
        SampleSparkConfigStep(),
        IsolatedEnvironmentTemplatesStep(),
    ]

    def __init__(self, base_path: Union[str, Path]) -> None:
        self.base_path: Path = Path(base_path).resolve()

    @classmethod
    def register_scaffold_step(cls, step: ScaffoldStep) -> None:
        if not isinstance(step, ScaffoldStep):
            raise TypeError("Step matches invalid protocol specifications.")
        cls._pipeline.append(step)

    def init_project(
        self,
        name: str,
        version: str,
        description: str,
        author: str,
        envs: Optional[List[str]] = None,
        pattern: Optional[str] = None,
        domains: Optional[List[str]] = None,
    ) -> None:
        """Initialize the target workspace structures transactionally.

        Args:
            name: Explicit name designation of the data platform workspace.
            version: Target initial configuration semantic version string.
            description: Concise purpose statement tracking the platform.
            author: Identity alias marking structural system ownership.
            envs: Optional list of isolated operational runtime environments.
            pattern: Targeted architectural data layout framework pattern.
            domains: Dedicated operational business domain areas for mesh.

        Raises:
            ProjectInitializationError: When any core stage execution crashes.
        """
        context = ScaffoldContext(
            root_path=self.base_path,
            name=name,
            version=version,
            description=description,
            author=author,
        )
        context.metadata["envs"] = envs if envs else ["dev", "qa", "prod"]
        context.metadata["pattern"] = pattern if pattern else "default"
        context.metadata["domains"] = domains if domains else []

        completed_stages: List[ScaffoldStep] = []
        for step in self._pipeline:
            try:
                step.execute(context)
                completed_stages.append(step)
            except Exception as error:
                self._abort_and_rollback(completed_stages, context)
                raise ProjectInitializationError(f"scaffolding transaction failed at '{step.name}': {error}") from error

    def _abort_and_rollback(self, completed_stages: List[ScaffoldStep], context: ScaffoldContext) -> None:
        for step in reversed(completed_stages):
            try:
                step.rollback(context)
            except Exception as rollback_error:
                logger.critical("Rollback routine failed: %s", rollback_error)
