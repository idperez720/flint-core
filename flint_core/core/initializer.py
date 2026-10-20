"""This module implements transactional project scaffolding engines for flint."""

import hashlib
import importlib.metadata
import logging
import os
import stat
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol, Type, Union, cast, runtime_checkable

import yaml

from flint_core.core.exceptions import ProjectInitializationError

logger = logging.getLogger(__name__)

LAYOUT_SUBDIRS: Dict[str, List[str]] = {
    "default": [],
    "medallion": ["bronze", "silver", "gold"],
    "kimball": ["staging", "dimensions", "facts"],
    "inmon": ["staging", "corporate_edw", "data_marts"],
    "datavault": ["raw_vault", "business_vault", "info_marts"],
}


@dataclass(frozen=True)
class _CreatedResource:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    digest: Optional[str] = None
    link_target: Optional[str] = None


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
        "_owned_paths",
        "retained_paths",
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
        self._owned_paths: Dict[Path, _CreatedResource] = {}
        self.retained_paths: List[Path] = []

    def check_path(self, path: Path, allow_final_symlink: bool = False) -> Path:
        """Checks lexical containment and refuses symlinks inside the project."""
        if ".." in path.parts:
            raise ValueError(f"Scaffold path cannot contain '..': {path}")
        if not path.is_absolute():
            path = self.root_path / path
        relative = path.relative_to(self.root_path)
        current = self.root_path
        candidates = [current]
        for component in relative.parts:
            current = current / component
            candidates.append(current)
        for candidate in candidates:
            if candidate.is_symlink() and not (allow_final_symlink and candidate == path):
                raise ValueError(f"Scaffold path cannot traverse a symlink: {candidate}")
            if candidate != path and candidate.exists() and not candidate.is_dir():
                raise NotADirectoryError(f"Scaffold parent is not a directory: {candidate}")
        return path

    def _remember(self, path: Path, status: os.stat_result) -> None:
        if path not in self._owned_paths:
            self.created_paths.append(path)
        self._owned_paths[path] = _CreatedResource(
            status.st_dev, status.st_ino, status.st_mode, status.st_size, status.st_mtime_ns
        )

    def _snapshot(self, path: Path) -> _CreatedResource:
        status = path.lstat()
        digest = None
        link_target = None
        if stat.S_ISREG(status.st_mode):
            with path.open("rb") as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino) != (status.st_dev, status.st_ino):
                    raise OSError(f"Scaffold resource changed while being inspected: {path}")
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
        elif stat.S_ISLNK(status.st_mode):
            link_target = str(path.readlink())
        return _CreatedResource(
            status.st_dev, status.st_ino, status.st_mode, status.st_size, status.st_mtime_ns, digest, link_target
        )

    def make_directory(self, path: Path, exclusive: bool = False) -> None:
        """Creates and records only directories allocated by this initialization."""
        path = self.check_path(path)
        relative = path.relative_to(self.root_path)
        current = self.root_path
        candidates = [current]
        for component in relative.parts:
            current = current / component
            candidates.append(current)
        for candidate in candidates:
            self.check_path(candidate)
            try:
                candidate.mkdir()
            except FileExistsError:
                if (exclusive and candidate == path) or candidate.is_symlink() or not candidate.is_dir():
                    raise
            else:
                self._remember(candidate, candidate.lstat())

    def write_text(self, path: Path, content: str) -> None:
        """Exclusively creates a file and journals it before the first write."""
        path = self.check_path(path)
        created = False
        try:
            with path.open("x", encoding="utf-8") as stream:
                self._remember(path, os.fstat(stream.fileno()))
                created = True
                stream.write(content)
        finally:
            if created:
                try:
                    current = self.check_path(path).lstat()
                    owned = self._owned_paths[path]
                    if (current.st_dev, current.st_ino) == (owned.device, owned.inode):
                        self._owned_paths[path] = self._snapshot(path)
                except (OSError, ValueError) as error:
                    logger.warning("Could not inspect partially written scaffold resource '%s': %s", path, error)

    def capture_environment(self) -> None:
        """Snapshots provider output only beneath the environment root we created."""
        environment = self.root_path / ".venv"
        owned = self._owned_paths.get(environment)
        try:
            self.check_path(environment)
            status = environment.lstat()
            if owned is None or (status.st_dev, status.st_ino) != (owned.device, owned.inode):
                logger.warning("Environment ownership is uncertain; preserving '%s'.", environment)
                return
            for parent, directories, files in os.walk(environment, followlinks=False):
                for name in directories + files:
                    path = self.check_path(Path(parent) / name, allow_final_symlink=True)
                    if path not in self._owned_paths:
                        self.created_paths.append(path)
                    self._owned_paths[path] = self._snapshot(path)
        except (OSError, ValueError) as error:
            logger.warning("Could not inspect environment resources at '%s': %s", environment, error)

    def capture_provider_file(self, path: Path) -> None:
        """Records an output reserved for the selected environment provider."""
        try:
            path = self.check_path(path)
            if path.exists():
                self._remember(path, path.lstat())
                self._owned_paths[path] = self._snapshot(path)
        except (OSError, ValueError) as error:
            logger.warning("Could not inspect provider output '%s': %s", path, error)

    def rollback_created(self, final: bool = False) -> None:
        """Removes unchanged owned resources, preserving replacements and unknown files."""
        for path in sorted(self._owned_paths, key=lambda item: len(item.parts), reverse=True):
            owned = self._owned_paths[path]
            try:
                self.check_path(path, allow_final_symlink=stat.S_ISLNK(owned.mode))
                current = path.lstat()
                if (current.st_dev, current.st_ino) != (owned.device, owned.inode):
                    raise ValueError("Resource identity changed")
                for parent in path.parents:
                    parent_resource = self._owned_paths.get(parent)
                    if parent_resource is not None:
                        parent_status = parent.lstat()
                        if (parent_status.st_dev, parent_status.st_ino) != (
                            parent_resource.device,
                            parent_resource.inode,
                        ):
                            raise ValueError("Parent resource identity changed")
                if stat.S_ISDIR(owned.mode):
                    path.rmdir()
                else:
                    if self._snapshot(path) != owned or (stat.S_ISREG(owned.mode) and owned.digest is None):
                        raise ValueError("Resource contents or metadata changed")
                    path.unlink()
            except FileNotFoundError:
                self._owned_paths.pop(path, None)
            except (OSError, ValueError) as error:
                if stat.S_ISDIR(owned.mode) and not final:
                    continue
                logger.warning("Preserving scaffold resource '%s': %s", path, error)
                self.retained_paths.append(path)
                self._owned_paths.pop(path, None)
            else:
                self._owned_paths.pop(path, None)


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
        context.write_text(toml_path, toml_content)

        req_path = context.root_path / "requirements.txt"
        context.write_text(req_path, f"flint-core{extras_buf}>={version_str}\n")

    def create_environment(self, context: ScaffoldContext) -> None:
        try:
            subprocess.run(
                ["uv", "venv", "--allow-existing", str(context.root_path / ".venv")],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.SubprocessError, FileNotFoundError) as err:
            raise RuntimeError("Failed to allocate virtual environment via 'uv'.") from err

    def install_dependencies(self, context: ScaffoldContext) -> None:
        """Invokes high-speed synchronization over uv pip boundaries."""
        interpreter = context.root_path / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        try:
            subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(interpreter),
                    "-r",
                    "requirements.txt",
                ],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"Failed to resolve sync dependencies via 'uv'. Details: {err.stderr.strip()}") from err


@EnvironmentRegistry.register("poetry")
class PoetryEnvironmentStrategy:
    """Handles modern deployment setups governed by poetry standards layouts."""

    def _environment(self) -> Dict[str, str]:
        """Scopes Poetry to the project instead of an inherited active environment."""
        environment = dict(os.environ)
        for name in ("VIRTUAL_ENV", "CONDA_PREFIX", "POETRY_ACTIVE"):
            environment.pop(name, None)
        environment["POETRY_VIRTUALENVS_IN_PROJECT"] = "true"
        environment["POETRY_VIRTUALENVS_CREATE"] = "true"
        return environment

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
        context.write_text(toml_path, content)

    def create_environment(self, context: ScaffoldContext) -> None:
        try:
            subprocess.run(
                [sys.executable, "-m", "venv", str(context.root_path / ".venv")],
                cwd=context.root_path,
                check=True,
                capture_output=True,
                env=self._environment(),
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
                env=self._environment(),
            )
        except subprocess.CalledProcessError as err:
            raise RuntimeError(
                f"Poetry lifestyle optimization environment lock failed. Details: {err.stderr.strip()}"
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
        context.write_text(toml_path, toml_content)

        req_path = context.root_path / "requirements.txt"
        context.write_text(req_path, f"flint-core{extras_buf}>={version_str}\n")

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
                f"Standard python fallback environment pip install crashed. Details: {err.stderr.strip()}"
            ) from err


class DirectoryStructureStep:
    """Core stage orchestrating allocation of physical folders layouts."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the structural descriptor matching layout components."""
        return "Directory Structure Layout Scaffolding"

    def target_paths(self, context: ScaffoldContext) -> List[Path]:
        """Lists the directories shared by preflight and generation."""
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

        return target_folders

    def execute(self, context: ScaffoldContext) -> None:
        """Allocates directories without claiming existing paths."""
        for folder in self.target_paths(context):
            context.make_directory(folder)

    def rollback(self, context: ScaffoldContext) -> None:
        """Removes allocated structural paths from target contexts."""
        context.rollback_created()


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

        manager_name = str(context.metadata.get("manager", "venv")).strip().lower()
        try:
            strategy = EnvironmentRegistry.resolve(manager_name)
        except ValueError as err:
            raise ProjectInitializationError(str(err)) from err

        strategy.write_manifests(context, version_str)
        context.make_directory(context.root_path / ".venv", exclusive=True)
        try:
            strategy.create_environment(context)
        finally:
            context.capture_environment()
        try:
            strategy.install_dependencies(context)
        finally:
            context.capture_environment()
            if manager_name == "poetry":
                context.capture_provider_file(context.root_path / "poetry.lock")

    def rollback(self, context: ScaffoldContext) -> None:
        """Retroactively unlinks manifests and cleans up environments."""
        context.rollback_created()


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
        context.write_text(csv_path, csv_content)

    def rollback(self, context: ScaffoldContext) -> None:
        """Unlinks physical file elements allocated on baseline storage."""
        context.rollback_created()


class SampleCatalogStep:
    """Core step linking the seed boilerplate onto decentral catalog structures."""

    __slots__ = ()

    @property
    def name(self) -> str:
        """Returns the semantic identification string token tag."""
        return "Seed Declarative Catalog Configuration Generation"

    def _write_file(self, path: Path, content: str, context: ScaffoldContext) -> None:
        context.write_text(path, textwrap.dedent(content))

    def target_paths(self, context: ScaffoldContext) -> List[Path]:
        """Lists generated manifests without opening or modifying them."""
        pattern = str(context.metadata.get("pattern", "default"))
        if pattern == "default":
            return [context.root_path / "conf" / "catalog" / "datasets.yaml"]
        if pattern in ("datamart", "datamesh"):
            domains = cast(List[str], context.metadata.get("domains", []))
            return [context.root_path / "conf" / "catalog" / domain / "sample_contracts.yaml" for domain in domains]
        return [
            context.root_path / "conf" / "catalog" / subdir / "sample_dataset.yaml"
            for subdir in LAYOUT_SUBDIRS.get(pattern, [])
        ]

    def execute(self, context: ScaffoldContext) -> None:
        """Compiles modular metadata declarations utilizing conventions."""
        pattern = str(context.metadata.get("pattern", "default"))

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

        if pattern in ("datamart", "datamesh"):
            contract_content = """\
                # Domain specific data contract layout parameters
                domain_dataset:
                  description: 'Domain template dataset framework reference'
                  format: 'parquet'
                  engine: 'pandas'
                  storage_path: 'data/domain_data.parquet'
            """
            content = contract_content
        else:
            content = generic_content
        sample_name, sample_body = next(iter(yaml.safe_load(textwrap.dedent(content)).items()))
        for path in self.target_paths(context):
            dataset_name = sample_name if pattern == "default" else f"{path.parent.name}.{sample_name}"
            self._write_file(path, yaml.safe_dump({dataset_name: sample_body}, sort_keys=False), context)

    def rollback(self, context: ScaffoldContext) -> None:
        """Flushes generated catalog files safely during cleanup maneuvers."""
        context.rollback_created()


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
        context.write_text(spark_path, spark_content)

    def rollback(self, context: ScaffoldContext) -> None:
        """Removes global properties layout contexts transactionally."""
        context.rollback_created()


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

            context.write_text(var_path, var_content)
            context.write_text(spark_path, spark_content)

    def rollback(self, context: ScaffoldContext) -> None:
        """Removes isolated templates across structural profiles blocks."""
        context.rollback_created()


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

        try:
            self._preflight(context)
        except (OSError, ValueError) as error:
            raise ProjectInitializationError(f"scaffolding transaction failed during preflight: {error}") from error

        started_stages: List[ScaffoldStep] = []
        for step in self._pipeline:
            started_stages.append(step)
            try:
                step.execute(context)
            except Exception as error:
                self._abort_and_rollback(started_stages, context)
                raise ProjectInitializationError(f"scaffolding transaction failed at '{step.name}': {error}") from error

    def _preflight(self, context: ScaffoldContext) -> None:
        """Rejects collisions and unsafe paths before generating files or running tools."""
        pattern = str(context.metadata["pattern"])
        if pattern not in {*LAYOUT_SUBDIRS, "datamart", "datamesh"}:
            raise ValueError(f"Unknown scaffold pattern: {pattern}")
        manager = str(context.metadata["manager"]).strip().lower()
        context.metadata["manager"] = manager
        EnvironmentRegistry.resolve(manager)
        for field in ("envs", "domains"):
            names = cast(List[str], context.metadata[field])
            if not isinstance(names, list):
                raise ValueError(f"Scaffold {field} must be a list of names.")
            for name in names:
                if not isinstance(name, str) or not name or name in (".", "..") or "/" in name or "\\" in name:
                    raise ValueError(f"Each {field} name must be a single path component: {name!r}")
            if len(names) != len(set(names)):
                raise ValueError(f"Duplicate {field} names are not allowed.")
        for path in DirectoryStructureStep().target_paths(context):
            context.check_path(path)
            if path.exists() and not path.is_dir():
                raise NotADirectoryError(f"Scaffold directory collides with an existing file: {path}")
        reserved = [
            context.root_path / "pyproject.toml",
            context.root_path / ".venv",
            context.root_path / "data" / "sample_table.csv",
            context.root_path / "conf" / "spark.yml",
            *SampleCatalogStep().target_paths(context),
        ]
        reserved.append(context.root_path / ("poetry.lock" if manager == "poetry" else "requirements.txt"))
        for environment in cast(List[str], context.metadata["envs"]):
            reserved.extend(
                context.root_path / "conf" / "envs" / environment / name for name in ("variables.yml", "spark.yml")
            )
        for path in reserved:
            context.check_path(path)
            if path.exists():
                raise FileExistsError(f"Scaffold target already exists: {path}")

    def _abort_and_rollback(self, started_stages: List[ScaffoldStep], context: ScaffoldContext) -> None:
        """Cleans all started steps, including partial output from the failing step."""
        for step in reversed(started_stages):
            try:
                step.rollback(context)
            except Exception as rollback_error:
                logger.critical("Rollback routine failed: %s", rollback_error)
        context.rollback_created(final=True)
