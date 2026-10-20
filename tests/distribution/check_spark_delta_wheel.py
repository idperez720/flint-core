"""Verify the Spark-first v1 wheel outside the checkout, initially without Pandas."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def check(wheel, work):
    environment = {key: value for key, value in os.environ.items() if key not in ("PYTHONPATH", "VIRTUAL_ENV")}
    venv = work / "venv"
    python = venv / "bin/python"
    flint = venv / "bin/flint"
    environment.update(PYTHONNOUSERSITE="1", SPARK_LOCAL_IP="127.0.0.1", PYSPARK_PYTHON=str(python))

    def command(arguments, expected=0):
        completed = subprocess.run(
            [str(argument) for argument in arguments],
            cwd=work,
            env=environment,
            capture_output=True,
            text=True,
            timeout=240,
        )
        if completed.returncode != expected:
            raise RuntimeError(f"Installed Spark wheel check failed: {completed.stdout}\n{completed.stderr}")
        return completed.stdout

    command(["uv", "venv", venv, "--python", sys.executable])
    command(["uv", "pip", "install", "--python", python, wheel])
    command(
        [
            python,
            "-c",
            f"""import importlib.util
from pathlib import Path
import flint_core
assert Path(flint_core.__file__).resolve().is_relative_to(Path({str(venv)!r}).resolve())
for name in ("pandas", "pyspark", "delta", "deltalake", "pyarrow"):
    assert importlib.util.find_spec(name) is None
""",
        ]
    )
    cli_project = work / "cli_project"
    python_project = work / "python_project"
    for root in (cli_project, python_project):
        command([flint, "init", "--format", "v1", "--engine", "spark", "--path", root, "--no-input"])
        assert json.loads(command([flint, "validate", "--project", root, "--json"]))["status"] == "valid"
        assert json.loads(command([flint, "run", "--project", root, "--dry-run", "--json"]))["engine"] == "spark"
        assert not (root / "data/customers").exists()
    command(
        [
            "uv",
            "pip",
            "install",
            "--python",
            python,
            str(wheel) + "[spark-delta]",
            "pyspark==4.1.2",
            "delta-spark==4.4.1",
        ]
    )
    command(
        [
            python,
            "-c",
            "import importlib.util; assert importlib.util.find_spec('pandas') is None; "
            "assert importlib.util.find_spec('pyarrow') is None",
        ]
    )
    summary = json.loads(command([flint, "run", "--project", cli_project, "--spark-local", "--json"]))
    assert summary["status"] == "success" and summary["publications"][0]["state"] == "committed"
    before = {path.name: path.read_bytes() for path in (cli_project / "data/customers/_delta_log").glob("*.json")}
    repeated = json.loads(command([flint, "run", "--project", cli_project, "--spark-local", "--json"], expected=2))
    assert repeated["status"] == "failed" and repeated["publications"] == []
    assert before == {
        path.name: path.read_bytes() for path in (cli_project / "data/customers/_delta_log").glob("*.json")
    }
    command(
        [
            python,
            "-c",
            f"""from flint_core import FlintProject
from flint_core.runtime.spark import open_session
from pyspark import SparkContext
with FlintProject.load({str(python_project)!r}) as project:
    result = project.run(spark_local=True)
    assert result.success, result.to_dict()
assert SparkContext._active_spark_context is None
session, owned = open_session(local=True)
try:
    left = session.read.format("delta").load({str(cli_project / "data/customers")!r}).orderBy("id").collect()
    right = session.read.format("delta").load({str(python_project / "data/customers")!r}).orderBy("id").collect()
    assert left == right
    assert [row.name for row in right] == ["Alice", "Bob"]
    with FlintProject.load({str(python_project)!r}) as project:
        assert not project.run(spark=session).success
    assert session.range(1).count() == 1
finally:
    session.stop()
assert owned
""",
        ]
    )
    command(
        [
            "uv",
            "pip",
            "install",
            "--python",
            python,
            str(wheel) + "[pandas,delta]",
            "pandas==3.0.3",
            "pyarrow==24.0.0",
            "deltalake==1.6.1",
        ]
    )
    command(
        [
            python,
            "-c",
            f"""from pathlib import Path
import yaml
from flint_core import FlintProject
from flint_core.runtime.scaffold import create_project
from deltalake import DeltaTable
root = Path({str(work / "pandas_delta")!r})
create_project(root)
manifest = root / "conf/bindings.dev.yaml"
bindings = yaml.safe_load(manifest.read_text())
bindings["bindings"]["curated.customers"].update(
    adapter="pandas.delta", format="delta", target={{"kind":"delta_table","path":"data/customers"}})
manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
with FlintProject.load(root) as project:
    result = project.run()
    assert result.success, result.to_dict()
    assert not project.run().success
table = DeltaTable(root / "data/customers")
assert table.version() == 0
assert table.to_pandas().sort_values("id")["name"].tolist() == ["Alice", "Bob"]
""",
        ]
    )
    versions = json.loads(
        command(
            [
                python,
                "-c",
                "import json; from importlib.metadata import version; "
                "print(json.dumps({name:version(name) for name in "
                "('flint-core','pyspark','delta-spark','pandas','deltalake','pyarrow')}))",
            ]
        )
    )
    return {
        "wheel": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "platform": sys.platform,
        "runtime_versions": versions,
        "metadata_without_engines": True,
        "spark_execution_without_pandas_or_pyarrow": True,
        "python_cli_equivalent": True,
        "borrowed_session_preserved": True,
        "owned_session_stopped": True,
        "second_create_preserves_table": True,
        "pandas_csv_to_delta": True,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="flint-spark-wheel-") as directory:
        report = check(arguments.wheel.resolve(), Path(directory))
    if arguments.report:
        arguments.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
