"""Verify the installed wheel in a fresh environment outside the source checkout."""

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
    environment["PYTHONNOUSERSITE"] = "1"
    venv = work / "venv"
    python = venv / "bin" / "python"
    flint = venv / "bin" / "flint"

    def command(arguments, expected=0):
        result = subprocess.run(
            [str(value) for value in arguments],
            cwd=work,
            env=environment,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != expected:
            raise RuntimeError(f"Distribution check failed: {result.stdout}\n{result.stderr}")
        return result.stdout

    command(["uv", "venv", venv, "--python", sys.executable])
    command(["uv", "pip", "install", "--python", python, wheel])
    command(
        [
            python,
            "-c",
            "import importlib.util; from pathlib import Path; import flint_core; "
            "from importlib.metadata import version; "
            f"assert Path(flint_core.__file__).resolve().is_relative_to(Path({str(venv)!r}).resolve()); "
            "assert importlib.util.find_spec('pandas') is None; "
            "assert flint_core.__version__ == version('flint-core')",
        ]
    )
    first = work / "cli_project"
    command([flint, "init", "--format", "v1", "--path", first, "--no-input"])
    valid = json.loads(command([flint, "validate", "--project", first, "--json"]))
    planned = json.loads(command([flint, "run", "--project", first, "--dry-run", "--json"]))
    assert valid["status"] == "valid" and planned["status"] == "planned"
    assert not (first / "data/customers.csv").exists()
    command(["uv", "pip", "install", "--python", python, str(wheel) + "[pandas]"])
    ran = json.loads(command([flint, "run", "--project", first, "--json"]))
    assert ran["status"] == "success"
    previous = (first / "data/customers.csv").read_bytes()
    failed = json.loads(command([flint, "run", "--project", first, "--json"], expected=2))
    assert failed["status"] == "failed"
    assert (first / "data/customers.csv").read_bytes() == previous
    second = work / "python_project"
    command([flint, "init", "--format", "v1", "--path", second, "--no-input"])
    command(
        [
            python,
            "-c",
            "from flint_core import FlintProject; "
            f"project = FlintProject.load({str(second)!r}); "
            "result = project.run(); assert result.success; project.close()",
        ]
    )
    assert (second / "data/customers.csv").read_bytes() == previous
    versions = json.loads(
        command(
            [
                python,
                "-c",
                "import json; from importlib.metadata import version; "
                "print(json.dumps({name: version(name) for name in "
                "('flint-core', 'pandas', 'numpy', 'PyYAML', 'click')}))",
            ]
        )
    )
    return {
        "wheel": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "platform": sys.platform,
        "runtime_versions": versions,
        "metadata_without_pandas": True,
        "dry_run_without_pandas": True,
        "installed_wheel_import_verified": True,
        "python_cli_equivalent": True,
        "second_create_preserves_data": True,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="flint-wheel-check-") as directory:
        report = check(arguments.wheel.resolve(), Path(directory))
    if arguments.report:
        arguments.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
