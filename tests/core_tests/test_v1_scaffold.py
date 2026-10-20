"""Executable scaffold and ownership-aware generation failures."""

import json
from importlib.metadata import version
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from flint_core import FlintProject, __version__
from flint_core.cli import entry_point
from flint_core.core.initializer import ScaffoldContext
from flint_core.runtime.errors import ProjectError
from flint_core.runtime.scaffold import create_project


def test_scaffold_validates_compiles_and_runs_via_both_entry_points(tmp_path):
    root = tmp_path / "example"
    response = CliRunner().invoke(entry_point, ["init", "--format", "v1", "--path", str(root), "--no-input"])
    assert response.exit_code == 0, response.output
    for command in (["validate"], ["run", "--dry-run"]):
        response = CliRunner().invoke(entry_point, [*command, "--project", str(root), "--json"])
        assert response.exit_code == 0, response.output
    assert not (root / "data/customers.csv").exists()
    with FlintProject.load(root) as project:
        assert project.run().success
    previous = (root / "data/customers.csv").read_bytes()
    response = CliRunner().invoke(entry_point, ["run", "--project", str(root), "--json"])
    assert response.exit_code == 2
    assert json.loads(response.output)["status"] == "failed"
    assert (root / "data/customers.csv").read_bytes() == previous
    assert b" Alice " not in previous


@pytest.mark.parametrize("engine", ["spark", "pandas"])
def test_generation_collisions_preserve_unrelated_files(tmp_path, engine):
    (tmp_path / "README.md").write_text("existing", encoding="utf-8")
    with pytest.raises(ProjectError):
        create_project(tmp_path, name="example", engine=engine)
    assert (tmp_path / "README.md").read_text() == "existing"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["README.md"]


@pytest.mark.parametrize("engine", ["spark", "pandas"])
def test_partial_generation_rolls_back_only_owned_resources(tmp_path, engine):
    (tmp_path / "unrelated").write_bytes(b"preserve")
    write = ScaffoldContext.write_text
    created = []

    def fail_after_write(context, path, contents):
        write(context, path, contents)
        created.append(path)
        if len(created) == 3:
            raise OSError("interrupted generation")

    with patch.object(ScaffoldContext, "write_text", fail_after_write):
        with pytest.raises(ProjectError):
            create_project(tmp_path, name="example", engine=engine)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["unrelated"]
    assert (tmp_path / "unrelated").read_bytes() == b"preserve"


@pytest.mark.parametrize("engine", ["spark", "pandas"])
def test_new_scaffold_never_invokes_environment_commands(tmp_path, engine):
    with patch("subprocess.run") as command:
        create_project(tmp_path, name="example", engine=engine)
    command.assert_not_called()


def test_package_and_advertised_metadata_versions_agree():
    import tomllib

    root = Path(__file__).resolve().parents[2]
    assert __version__ == tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    try:
        installed = version("flint-core")
    except Exception:
        return
    assert installed == __version__
