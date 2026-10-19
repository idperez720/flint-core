"""Explicit, isolated project configuration and pipeline factories."""

import importlib
import re
import sys
import tomllib
from pathlib import Path
from threading import RLock
from types import MappingProxyType, ModuleType
from typing import Mapping, Union
from uuid import uuid4

from flint_core.core.catalog.v1 import ManifestCatalog
from flint_core.core.exceptions import CatalogParseError
from flint_core.runtime.errors import ProjectError
from flint_core.runtime.models import Pipeline

_FACTORY = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", re.ASCII)


def _fields(value, required, optional=()):
    if not isinstance(value, dict) or set(required) - value.keys() or value.keys() - set(required) - set(optional):
        raise ProjectError("Missing or unknown project configuration fields.")
    return value


class FlintProject:
    """A project owns metadata and its registry, never a global active context."""

    @classmethod
    def load(cls, path: Union[str, Path] = ".", *, environment: str = "dev"):
        root = Path(path).resolve()
        try:
            with (root / "pyproject.toml").open("rb") as stream:
                configuration = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError):
            raise ProjectError("Cannot read project pyproject.toml.") from None
        tool = configuration.get("tool", {})
        if not isinstance(tool, dict):
            raise ProjectError("Invalid tool configuration.")
        data = _fields(
            tool.get("flint"),
            ("schema_version", "project_name", "pipeline_factory", "definitions", "environments"),
            ("source_dir", "default_pipeline"),
        )
        if type(data["schema_version"]) is not int or data["schema_version"] != 1:
            raise ProjectError("Unsupported project schema_version.")
        if not isinstance(data["project_name"], str) or not data["project_name"].strip():
            raise ProjectError("project_name must be a non-empty string.")
        factory = data["pipeline_factory"]
        if not isinstance(factory, str) or _FACTORY.fullmatch(factory) is None:
            raise ProjectError("pipeline_factory must be an explicit module:function.")
        environments = data["environments"]
        if not isinstance(environments, dict) or not isinstance(environment, str) or environment not in environments:
            raise ProjectError("Unknown project environment.")
        for settings in environments.values():
            _fields(settings, ("bindings",), ("variables",))
        selected = environments[environment]
        source_dir = data.get("source_dir", "src")
        if not isinstance(source_dir, str) or not source_dir or Path(source_dir).is_absolute():
            raise ProjectError("source_dir must be project-relative.")
        source = (root / source_dir).resolve()
        if not source.is_relative_to(root):
            raise ProjectError("source_dir cannot escape the project root.")
        default = data.get("default_pipeline")
        if default is not None and (not isinstance(default, str) or not default.strip()):
            raise ProjectError("default_pipeline must be a non-empty name.")
        try:
            catalog = ManifestCatalog(
                project_root=root,
                definitions=data["definitions"],
                bindings=selected["bindings"],
                environment=environment,
                variables=selected.get("variables"),
            )
        except (CatalogParseError, TypeError, ValueError):
            raise ProjectError("Invalid project catalog declarations.") from None
        return cls(root, data["project_name"], factory, source, default, catalog)

    def __init__(self, root, name, factory, source, default, catalog) -> None:
        self.root = root
        self.name = name
        self.catalog = catalog
        self.environment = catalog.snapshot.environment
        self._factory = factory
        self._source = source
        self._default = default
        self._namespace = "_flint_project_" + uuid4().hex
        self._pipelines = None
        self._closed = False
        self._lock = RLock()

    def _check_open(self) -> None:
        if self._closed:
            raise ProjectError("Project is closed.")

    def _clear_modules(self) -> None:
        for name in tuple(sys.modules):
            if name == self._namespace or name.startswith(self._namespace + "."):
                sys.modules.pop(name, None)

    @property
    def pipelines(self) -> Mapping[str, Pipeline]:
        with self._lock:
            self._check_open()
            if self._pipelines is None:
                module_name, factory_name = self._factory.split(":")
                package = ModuleType(self._namespace)
                package.__path__ = [str(self._source)]
                sys.modules[self._namespace] = package
                try:
                    module = importlib.import_module(self._namespace + "." + module_name)
                    factory = getattr(module, factory_name)
                    declared = factory()
                    if not isinstance(declared, (list, tuple)) or not declared:
                        raise ProjectError("Factory must return a non-empty pipeline list or tuple.")
                    registry = {}
                    for pipeline in declared:
                        if (
                            not isinstance(pipeline, Pipeline)
                            or not isinstance(pipeline.name, str)
                            or not pipeline.name
                        ):
                            raise ProjectError("Factory returned an invalid pipeline.")
                        if pipeline.name in registry:
                            raise ProjectError("Duplicate pipeline name.")
                        registry[pipeline.name] = pipeline
                    self._pipelines = MappingProxyType(registry)
                except Exception:
                    self._clear_modules()
                    raise ProjectError("Cannot load the explicit project pipeline factory.") from None
            return self._pipelines

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._clear_modules()

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, *arguments):
        self.close()
