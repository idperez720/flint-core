"""Regression checks for the Runtime/Catalog scope without Utilities."""

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

import flint_core
from flint_core.core.base import BaseEngine, EngineRegistry
from flint_core.core.catalog import DataCatalog
from flint_core.core.pipeline import Node


@pytest.mark.parametrize("name", ["deduplication", "scd2"])
def test_top_level_utility_exports_are_removed(name: str) -> None:
    assert name not in flint_core.__all__
    assert not hasattr(flint_core, name)


@pytest.mark.parametrize(
    "module_name",
    [
        "flint_core.functions",
        "flint_core.functions.deduplication",
        "flint_core.functions.scd2",
        "flint_core.pandas_core.deduplication",
        "flint_core.pandas_core.scd2",
    ],
)
def test_utility_modules_are_no_longer_importable(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


@pytest.mark.parametrize("name", ["latest", "first", "by_order", "combined", "scd2_process"])
def test_utility_methods_are_not_part_of_engine_contract(name: str) -> None:
    assert not hasattr(BaseEngine, name)
    assert not hasattr(EngineRegistry.get_engine("pandas"), name)


def test_io_engine_contract_only_requires_load_and_save() -> None:
    assert BaseEngine.__abstractmethods__ == {"load", "save"}


def test_engine_still_supports_instance_extensions() -> None:
    engine = EngineRegistry.get_engine("pandas")
    engine.custom_setting = "caller-owned"
    assert engine.custom_setting == "caller-owned"


def test_pipeline_runs_a_user_transformation_without_utilities(tmp_path: Path) -> None:
    pandas = pytest.importorskip("pandas")
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "core-scope"\n', encoding="utf-8")
    manifest = tmp_path / "catalog.yaml"
    manifest.write_text(
        "source:\n  engine: pandas\n  format: csv\n  storage_path: data/source.csv\n"
        "result:\n  engine: pandas\n  format: csv\n  storage_path: data/result.csv\n",
        encoding="utf-8",
    )
    (tmp_path / "data").mkdir()
    catalog = DataCatalog(manifest)
    catalog.save(pandas.DataFrame({"id": [1, 2], "value": [10, 20]}), "source")

    def transform(source):
        return source.assign(value=source["value"] * 2)

    Node("double_values", transform, ["source"], ["result"]).run(catalog=catalog)
    actual = catalog.load("result")
    assert actual.to_dict("list") == {"id": [1, 2], "value": [20, 40]}


def test_metadata_and_cli_do_not_require_optional_engines(tmp_path: Path) -> None:
    package_root = Path(flint_core.__file__).resolve().parent.parent
    script = """
import builtins
from pathlib import Path

original_import = builtins.__import__

def core_only(name, *args, **kwargs):
    if name.split(".")[0] in ("pandas", "pyspark"):
        raise ImportError("Optional engine unavailable")
    return original_import(name, *args, **kwargs)

builtins.__import__ = core_only

from click.testing import CliRunner
from flint_core.cli import entry_point
from flint_core.core.catalog import DataCatalog

assert DataCatalog(Path.cwd()).dataset_names == []
assert CliRunner().invoke(entry_point, ["--help"]).exit_code == 0
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(package_root)},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_spark_engine_does_not_expose_utility_methods() -> None:
    pytest.importorskip("pyspark")
    from flint_core.spark_core.engine import SparkEngine

    engine = SparkEngine()
    for name in ("latest", "first", "by_order", "combined", "scd2_process"):
        assert not hasattr(engine, name)
