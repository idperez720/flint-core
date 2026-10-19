"""Project configuration and isolated factory registration regressions."""

from pathlib import Path

import pytest

from flint_core import FlintProject
from flint_core.runtime.errors import ProjectError


def make_project(root: Path, *, factory=None, default=True):
    root.mkdir(parents=True, exist_ok=True)
    (root / "src" / "project_code").mkdir(parents=True, exist_ok=True)
    (root / "src" / "project_code" / "__init__.py").write_text("", encoding="utf-8")
    (root / "src" / "project_code" / "registry.py").write_text(
        factory or 'from flint_core import Pipeline\ndef build():\n    return [Pipeline("main", [])]\n',
        encoding="utf-8",
    )
    (root / "pyproject.toml").write_text(
        '[tool.flint]\nschema_version = 1\nproject_name = "test"\n'
        'pipeline_factory = "project_code.registry:build"\ndefinitions = ["definitions.yaml"]\n'
        + ('default_pipeline = "main"\n' if default else "")
        + '[tool.flint.environments.dev]\nbindings = ["bindings.yaml"]\n',
        encoding="utf-8",
    )
    (root / "definitions.yaml").write_text(
        "schema_version: 1\nkind: DatasetDefinitions\ndatasets:\n"
        "  source:\n    schema:\n      extra_columns: forbid\n"
        "      columns:\n        id: {type: int64, nullable: false}\n"
        "  result:\n    schema:\n      extra_columns: forbid\n"
        "      columns:\n        id: {type: int64, nullable: false}\n",
        encoding="utf-8",
    )
    (root / "bindings.yaml").write_text(
        "schema_version: 1\nkind: DatasetBindings\nenvironment: dev\nbindings:\n"
        "  source:\n    adapter: pandas.csv\n    format: csv\n"
        "    target: {kind: file, path: data/source.csv}\n    access: read_only\n"
        "  result:\n    adapter: pandas.csv\n    format: csv\n"
        "    target: {kind: file, path: data/result.csv}\n    access: read_write\n",
        encoding="utf-8",
    )
    return root


def test_metadata_loading_does_not_import_factory(tmp_path):
    root = make_project(tmp_path, factory='raise RuntimeError("factory must not be imported")')
    project = FlintProject.load(root, environment="dev")
    assert "source" in project.catalog.snapshot.definitions
    with pytest.raises(ProjectError):
        project.pipelines


def test_same_module_names_are_isolated_between_projects(tmp_path):
    first = FlintProject.load(make_project(tmp_path / "first"))
    second = FlintProject.load(make_project(tmp_path / "second"))
    assert first.pipelines["main"] is not second.pipelines["main"]
    assert (
        first.catalog.snapshot.bindings["source"].target.path != second.catalog.snapshot.bindings["source"].target.path
    )
    first.close()
    assert second.pipelines["main"].name == "main"


def test_unknown_environment_and_duplicate_registration_fail(tmp_path):
    root = make_project(tmp_path)
    with pytest.raises(ProjectError):
        FlintProject.load(root, environment="missing")
    root = make_project(
        tmp_path,
        factory='from flint_core import Pipeline\ndef build():\n return [Pipeline("x", []), Pipeline("x", [])]',
    )
    with pytest.raises(ProjectError):
        FlintProject.load(root).pipelines


def test_root_and_registry_do_not_follow_invocation_directory(tmp_path, monkeypatch):
    root = make_project(tmp_path / "project")
    monkeypatch.chdir(tmp_path)
    with FlintProject.load(root) as project:
        assert project.root == root
        assert list(project.pipelines) == ["main"]
    with pytest.raises(ProjectError):
        project.pipelines


def test_unknown_configuration_fields_fail(tmp_path):
    root = make_project(tmp_path)
    manifest = root / "pyproject.toml"
    manifest.write_text(manifest.read_text() + "unknown = true\n", encoding="utf-8")
    with pytest.raises(ProjectError):
        FlintProject.load(root)
