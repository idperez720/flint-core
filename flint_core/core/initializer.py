"""This module implements transactional project scaffolding engines for flint."""

import importlib.metadata
import logging
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol, Type, Union, cast, runtime_checkable

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
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        """Initializes the context container tracking scaffolding workflow state.

        Args:
            root_path: Target root folder location for project layout.
            name: Explicit identity designation of the data platform workspace.
            version: Target initial configuration semantic version string.
            description: Statement describing pipeline repository targets.
            author: Identity token alias marking system owners fields.
            metadata: Custom storage layer supporting extensible context fields.
        """
        self.root_path: Path = root_path.resolve()
        self.name: str = name
        self.version: str = version
        self.description: str = description
        self.author: str = author
        self.metadata: Dict[str, object] = metadata if metadata is not None else {}
        self.created_paths: List[Path] = []


@runtime_checkable
class ScaffoldStep(Protocol):
    """Structural protocol governing atomic project initialization stages."""

    @property
    def name(self) -> str:
        """The distinct identifying designation tracking execution steps."""
        ...

    def execute(self, context: ScaffoldContext) -> None:
        """Executes the specific atomic scaffolding generation logic."""
        ...

    def rollback(self, context: ScaffoldContext) -> None:
        """Reverts physical changes if downstream boundaries crash."""
        ...


@runtime_checkable
class EnvironmentStrategy(Protocol):
    """Structural protocol governing dynamic runtime environment creation."""

    def write_manifests(self, context: ScaffoldContext, version_str: str) -> None:
        """Writes configuration manifests matching the package manager type."""
        ...

    def create_environment(self, context: ScaffoldContext) -> None:
        """Executes the command routines to allocate a virtual environment."""
        ...

    def install_dependencies(self, context: ScaffoldContext) -> None:
        """Installs the framework and its chosen extras inside the allocated env."""
        ...


class EnvironmentRegistry:
    """Central lookup map routing environment strategy resolutions dynamically."""

    _registry: Dict[str, Type[EnvironmentStrategy]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[Type[EnvironmentStrategy]], Type[EnvironmentStrategy]]:
        """Decorator to map package management strategies safely to the registry."""

        def decorator(
            strategy_cls: Type[EnvironmentStrategy],
        ) -> Type[EnvironmentStrategy]:
            cls._registry[name.lower()] = strategy_cls
            return strategy_cls

        return decorator

    @classmethod
    def resolve(cls, name: str) -> EnvironmentStrategy:
        """Resolves target strategies or throws clean configuration errors."""
        strategy_cls = cls._registry.get(name.lower())
        if not strategy_cls:
            supported = ", ".join(cls._registry.keys())
            raise ValueError(f"Unsupported environment manager '{name}'. Supported: {supported}")
        return strategy_cls()


@EnvironmentRegistry.register("uv")
class UvEnvironmentStrategy:
    """Handles high-performance PEP 621 configurations and environments via uv."""

    def write_manifests(self, context: ScaffoldContext, version_str: str) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError("A configuration manifest file already exists.")

        extras: List[str] = cast(List[str], context.metadata.get("extras", []))
        extras_buf = f"[{','.join(extras)}]" if extras else ""

        toml_content = textwrap.dedent(f"""\
            [project]
            name = "{context.name}"
            version = "{context.version}"
            description = "{context.description}"
            authors = [
                {{name = "{context.author}"}}
            ]
            requires-python = ">=3.11,<4.0.0"
            dependencies = [
                "flint-core{extras_buf}>={version_str}",
            ]
        """)
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(toml_content)

        req_path = context.root_path / "requirements.txt"
        with open(req_path, "w", encoding="utf-8") as file:
            file.write(f"flint-core{extras_buf}>={version_str}\n")

    def create_environment(self, context: ScaffoldContext) -> None:
        try:
            subprocess.run(
                ["uv", "venv", ".venv"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.SubprocessError, FileNotFoundError) as err:
            raise RuntimeError("Failed to allocate virtual environment via 'uv'.") from err

    def install_dependencies(self, context: ScaffoldContext) -> None:
        """Invokes high-speed synchronization over uv pip boundaries."""
        try:
            subprocess.run(
                ["uv", "pip", "install", "-r", "requirements.txt"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            raise RuntimeError(
                f"Failed to resolve sync dependencies via 'uv'. "
                f"Details: {err.stderr.strip()}"
            ) from err


@EnvironmentRegistry.register("poetry")
class PoetryEnvironmentStrategy:
    """Handles modern deployment setups governed by poetry standards layouts."""

    def write_manifests(self, context: ScaffoldContext, version_str: str) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError("A configuration manifest file already exists.")

        extras: List[str] = cast(List[str], context.metadata.get("extras", []))
        if extras:
            extras_formatted = ", ".join(f'"{e}"' for e in extras)
            flint_dependency = f'flint-core = {{ version = "^{version_str}", extras = [{extras_formatted}] }}'
        else:
            flint_dependency = f'flint-core = "^{version_str}"'

        content = textwrap.dedent(f"""\
            [tool.poetry]
            name = "{context.name}"
            version = "{context.version}"
            description = "{context.description}"
            authors = ["{context.author}"]

            [tool.poetry.dependencies]
            python = ">=3.11,<4.0.0"
            {flint_dependency}

            [build-system]
            requires = ["poetry-core"]
            build-backend = "poetry.core.masonry.api"
        """)
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(content)

    def create_environment(self, context: ScaffoldContext) -> None:
        try:
            subprocess.run(
                ["poetry", "config", "virtualenvs.in-project", "true"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["poetry", "env", "use", "python3"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
            )
        except (subprocess.SubprocessError, FileNotFoundError) as err:
            raise RuntimeError("Failed to allocate virtual environment via 'poetry'.") from err

    def install_dependencies(self, context: ScaffoldContext) -> None:
        """Executes full locking and dependency installation via poetry."""
        try:
            subprocess.run(
                ["poetry", "install"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            raise RuntimeError(
                f"Poetry lifestyle optimization environment lock failed. "
                f"Details: {err.stderr.strip()}"
            ) from err


@EnvironmentRegistry.register("venv")
@EnvironmentRegistry.register("pip")
class VenvEnvironmentStrategy:
    """Fallback strategy allocating vanilla system virtual environments."""

    def write_manifests(self, context: ScaffoldContext, version_str: str) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError("A configuration manifest file already exists.")

        extras: List[str] = cast(List[str], context.metadata.get("extras", []))
        extras_buf = f"[{','.join(extras)}]" if extras else ""

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

        req_path = context.root_path / "requirements.txt"
        with open(req_path, "w", encoding="utf-8") as file:
            file.write(f"flint-core{extras_buf}>={version_str}\n")

    def create_environment(self, context: ScaffoldContext) -> None:
        try:
            subprocess.run(
                [sys.executable, "-m", "venv", ".venv"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
            )
        except subprocess.SubprocessError as err:
            raise RuntimeError("Failed to instantiate python standard library venv.") from err

    def install_dependencies(self, context: ScaffoldContext) -> None:
        """Performs traditional python pip package installation loop."""
        bindir = "Scripts" if sys.platform == "win32" else "bin"
        pip_path = context.root_path / ".venv" / bindir / "pip"
        try:
            subprocess.run(
                [str(pip_path), "install", "-r", "requirements.txt"],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            raise RuntimeError(
                f"Standard python fallback environment pip install crashed. "
                f"Details: {err.stderr.strip()}"
            ) from err


class DirectoryStructureStep:
    """Core stage orchestrating allocation of physical folders layouts."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the structural descriptor matching layout components."""
        return "Directory Structure Layout Scaffolding"

    def execute(self, context: ScaffoldContext) -> None:
        """Allocates targeted folder configurations tracking choice patterns."""
        pattern = str(context.metadata.get("pattern", "default"))
        envs = context.metadata.get("envs")
        if not isinstance(envs, list):
            envs = ["dev", "qa", "prod"]

        domains = context.metadata.get("domains")
        if not isinstance(domains, list):
            domains = []

        target_folders = [
            context.root_path / "src" / "notebooks",
            context.root_path / "data",
            context.root_path / "conf" / "catalog",
        ]

        for env in envs:
            target_folders.append(context.root_path / "conf" / "envs" / str(env))

        if pattern in ("datamart", "datamesh"):
            for domain in domains:
                target_folders.append(context.root_path / "conf" / "catalog" / str(domain))
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
        """Removes allocated structural paths from target contexts."""
        for item in reversed(context.created_paths):
            if item.is_dir() and item.exists() and not any(item.iterdir()):
                item.rmdir()


class WorkspaceEnvironmentStep:
    """Orchestrates configuration manifests and virtual environment setups."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Workspace Isolation and Environment Configuration Setup"

    def execute(self, context: ScaffoldContext) -> None:
        try:
            version_str = importlib.metadata.version("flint-core")
        except importlib.metadata.PackageNotFoundError:
            version_str = "0.1.0"

        manager_name = str(context.metadata.get("manager", "venv"))
        try:
            strategy = EnvironmentRegistry.resolve(manager_name)
        except ValueError as err:
            raise ProjectInitializationError(str(err)) from err

        strategy.write_manifests(context, version_str)

        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            context.created_paths.append(toml_path)

        req_path = context.root_path / "requirements.txt"
        if req_path.exists():
            context.created_paths.append(req_path)

        strategy.create_environment(context)
        venv_path = context.root_path / ".venv"
        if venv_path.exists():
            context.created_paths.append(venv_path)

        strategy.install_dependencies(context)

    def rollback(self, context: ScaffoldContext) -> None:
        """Retroactively unlinks manifests and cleans up environments."""
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.is_file() and toml_path.exists():
            toml_path.unlink()

        req_path = context.root_path / "requirements.txt"
        if req_path.is_file() and req_path.exists():
            req_path.unlink()

        venv_path = context.root_path / ".venv"
        if venv_path.is_dir() and venv_path.exists():
            shutil.rmtree(venv_path, ignore_errors=True)


class SampleDataStep:
    """Optional baseline step dropping an operational seed table asset."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the identifier tracking data."""
        return "Boilerplate Seed Sample Physical Data Insertion"

    def execute(self, context: ScaffoldContext) -> None:
        """Writes transactional baseline mock tracking metrics profiles."""
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
        """Unlinks physical file elements allocated on baseline storage."""
        csv_path = context.root_path / "data" / "sample_table.csv"
        if csv_path.is_file() and csv_path.exists():
            csv_path.unlink()


class SampleCatalogStep:
    """Core step linking the seed boilerplate onto decentral catalog structures."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the semantic identification string token tag."""
        return "Seed Declarative Catalog Configuration Generation"

    def _write_file(self, path: Path, content: str, context: ScaffoldContext) -> None:
        with open(path, "w", encoding="utf-8") as file:
            file.write(textwrap.dedent(content))
        context.created_paths.append(path)

    def execute(self, context: ScaffoldContext) -> None:
        """Compiles modular metadata declarations utilizing conventions."""
        pattern = str(context.metadata.get("pattern", "default"))
        domains = context.metadata.get("domains")
        if not isinstance(domains, list):
            domains = []

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
                path = context.root_path / "conf" / "catalog" / str(domain) / "sample_contracts.yaml"
                self._write_file(path, contract_content, context)
        else:
            subdirs = LAYOUT_SUBDIRS.get(pattern, [])
            for subdir in subdirs:
                path = context.root_path / "conf" / "catalog" / subdir / "sample_dataset.yaml"
                self._write_file(path, generic_content, context)

    def rollback(self, context: ScaffoldContext) -> None:
        """Flushes generated catalog files safely during cleanup maneuvers."""
        pattern = str(context.metadata.get("pattern", "default"))
        domains = context.metadata.get("domains")
        if not isinstance(domains, list):
            domains = []

        paths_to_delete = []
        if pattern == "default":
            paths_to_delete.append(context.root_path / "conf" / "catalog" / "datasets.yaml")
        elif pattern in ("datamart", "datamesh"):
            for domain in domains:
                paths_to_delete.append(context.root_path / "conf" / "catalog" / str(domain) / "sample_contracts.yaml")
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
        """Returns the formal categorization metadata identifier."""
        return "Seed Spark Configuration Template Generation"

    def execute(self, context: ScaffoldContext) -> None:
        """Generates configuration mappings for distributed storage engines."""
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
        """Removes global properties layout contexts transactionally."""
        spark_path = context.root_path / "conf" / "spark.yml"
        if spark_path.is_file() and spark_path.exists():
            spark_path.unlink()


class IsolatedEnvironmentTemplatesStep:
    """Generates isolated environment configuration templates automatically."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the semantic token naming label identifying target layers."""
        return "Isolated Environment Sandbox Configuration Generation"

    def execute(self, context: ScaffoldContext) -> None:
        """Builds multi-environment properties tuning maps per sandbox tier."""
        envs = context.metadata.get("envs")
        if not isinstance(envs, list):
            envs = ["dev", "qa", "prod"]

        for env in envs:
            var_path = context.root_path / "conf" / "envs" / str(env) / "variables.yml"
            spark_path = context.root_path / "conf" / "envs" / str(env) / "spark.yml"

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
        """Removes isolated templates across structural profiles blocks."""
        envs = context.metadata.get("envs")
        if not isinstance(envs, list):
            envs = ["dev", "qa", "prod"]

        for env in envs:
            var_path = context.root_path / "conf" / "envs" / str(env) / "variables.yml"
            spark_path = context.root_path / "conf" / "envs" / str(env) / "spark.yml"
            if var_path.is_file() and var_path.exists():
                var_path.unlink()
            if spark_path.is_file() and spark_path.exists():
                spark_path.unlink()


class ProjectInitializer:
    """Orchestrates modular pipeline stages under rigid transaction blocks."""

    _pipeline: List[ScaffoldStep] = [
        DirectoryStructureStep(),
        WorkspaceEnvironmentStep(),
        SampleDataStep(),
        SampleCatalogStep(),
        SampleSparkConfigStep(),
        IsolatedEnvironmentTemplatesStep(),
    ]

    def __init__(self, base_path: Union[str, Path]) -> None:
        """Initializes the baseline orchestrator pointing to project pathways."""
        self.base_path: Path = Path(base_path).resolve()

    @classmethod
    def register_scaffold_step(cls, step: ScaffoldStep) -> None:
        """Allows core workflow extensions to append processing hooks steps."""
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
        manager: Optional[str] = None,
        extras: Optional[List[str]] = None,  # <- Añadido
    ) -> None:
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
        context.metadata["manager"] = manager if manager else "venv"
        context.metadata["extras"] = extras if extras else []  # <- Asignado

        completed_stages: List[ScaffoldStep] = []
        for step in self._pipeline:
            try:
                step.execute(context)
                completed_stages.append(step)
            except Exception as error:
                self._abort_and_rollback(completed_stages, context)
                raise ProjectInitializationError(f"scaffolding transaction failed at '{step.name}': {error}") from error

    def _abort_and_rollback(self, completed_stages: List[ScaffoldStep], context: ScaffoldContext) -> None:
        """Triggers retroactive cleanups across completed pipeline branches."""
        for step in reversed(completed_stages):
            try:
                step.rollback(context)
            except Exception as rollback_error:
                logger.critical("Rollback routine failed: %s", rollback_error)
