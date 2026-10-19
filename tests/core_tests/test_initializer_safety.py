"""Regression tests for initializer ownership, collisions, and partial failures."""

import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flint_core.core.initializer import (
    DirectoryStructureStep,
    EnvironmentRegistry,
    IsolatedEnvironmentTemplatesStep,
    ProjectInitializationError,
    ProjectInitializer,
    SampleCatalogStep,
    SampleDataStep,
    SampleSparkConfigStep,
    ScaffoldContext,
    WorkspaceEnvironmentStep,
)


def initialize(root: Path, **options) -> None:
    ProjectInitializer(root).init_project("safety", "0.1.0", "Safe initialization", "Tester", **options)


def successful_command(*arguments, **options) -> MagicMock:
    root = options["cwd"]
    environment = root / ".venv"
    environment.mkdir(exist_ok=True)
    (environment / "bin").mkdir(exist_ok=True)
    (environment / "bin" / "python").write_text("managed fixture", encoding="utf-8")
    return MagicMock()


def test_workspace_rollback_preserves_resources_it_did_not_create(tmp_path: Path) -> None:
    requirements = tmp_path / "requirements.txt"
    manifest = tmp_path / "pyproject.toml"
    requirements.write_text("existing requirement", encoding="utf-8")
    manifest.write_text("existing project", encoding="utf-8")
    environment = tmp_path / ".venv"
    environment.mkdir()
    sentinel = environment / "sentinel"
    sentinel.write_text("existing environment", encoding="utf-8")
    context = ScaffoldContext(tmp_path, "safety", "0.1.0", "Safety", "Tester")

    WorkspaceEnvironmentStep().rollback(context)

    assert requirements.read_text(encoding="utf-8") == "existing requirement"
    assert manifest.read_text(encoding="utf-8") == "existing project"
    assert sentinel.read_text(encoding="utf-8") == "existing environment"


@pytest.mark.parametrize(
    ("manager", "relative_path"),
    [
        ("venv", "requirements.txt"),
        ("uv", "requirements.txt"),
        ("poetry", "poetry.lock"),
        ("venv", "data/sample_table.csv"),
        ("venv", "conf/catalog/datasets.yaml"),
        ("venv", "conf/spark.yml"),
        ("venv", "conf/envs/dev/variables.yml"),
        ("venv", "conf/envs/dev/spark.yml"),
    ],
)
def test_generated_file_collision_is_rejected_before_commands(tmp_path: Path, manager: str, relative_path: str) -> None:
    existing = tmp_path / relative_path
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b"existing contents")

    with patch("subprocess.run", side_effect=successful_command) as command:
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path, manager=manager)

    assert existing.read_bytes() == b"existing contents"
    assert not (tmp_path / "pyproject.toml").exists()
    command.assert_not_called()


@pytest.mark.parametrize("manager", ["venv", "uv", "poetry"])
def test_existing_environment_is_never_reused_or_removed(tmp_path: Path, manager: str) -> None:
    environment = tmp_path / ".venv"
    environment.mkdir()
    sentinel = environment / "sentinel"
    sentinel.write_text("keep", encoding="utf-8")

    with patch("subprocess.run", side_effect=successful_command) as command:
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path, manager=manager)

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert not (tmp_path / "pyproject.toml").exists()
    command.assert_not_called()


@pytest.mark.parametrize(
    "step_type",
    [
        DirectoryStructureStep,
        WorkspaceEnvironmentStep,
        SampleDataStep,
        SampleCatalogStep,
        SampleSparkConfigStep,
        IsolatedEnvironmentTemplatesStep,
    ],
)
def test_the_failing_step_is_included_in_cleanup(tmp_path: Path, step_type: type) -> None:
    original_execute = step_type.execute

    def fail_after_creation(step, context):
        original_execute(step, context)
        raise OSError("injected failure after resource creation")

    with (
        patch("subprocess.run", side_effect=successful_command),
        patch.object(step_type, "execute", new=fail_after_creation),
    ):
        with pytest.raises(ProjectInitializationError, match="injected failure"):
            initialize(tmp_path)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("manager", ["venv", "uv", "poetry"])
def test_partial_environment_creation_failure_removes_only_new_resources(tmp_path: Path, manager: str) -> None:
    unrelated = tmp_path / "notes.txt"
    unrelated.write_text("keep", encoding="utf-8")

    def partial_environment(*arguments, **options):
        successful_command(*arguments, **options)
        raise subprocess.CalledProcessError(1, arguments[0], stderr="failed during creation")

    with patch("subprocess.run", side_effect=partial_environment):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path, manager=manager)

    assert list(tmp_path.iterdir()) == [unrelated]
    assert unrelated.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("relative_path", ["data", "conf", "src"])
def test_directory_symlinks_cannot_redirect_generation(tmp_path: Path, relative_path: str) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / relative_path).symlink_to(outside, target_is_directory=True)

    with patch("subprocess.run", side_effect=successful_command) as command:
        with pytest.raises(ProjectInitializationError):
            initialize(root)

    assert list(outside.iterdir()) == []
    assert (root / relative_path).is_symlink()
    command.assert_not_called()


@pytest.mark.parametrize("field", ["envs", "domains"])
@pytest.mark.parametrize("name", ["../outside", "absolute", "..", "nested/name", "nested\\name"])
def test_environment_and_domain_names_cannot_escape_the_root(tmp_path: Path, field: str, name: str) -> None:
    if name == "absolute":
        name = str(tmp_path.parent / f"outside-{tmp_path.name}")
    with patch("subprocess.run", side_effect=successful_command) as command:
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path, pattern="datamesh", **{field: [name]})

    assert list(tmp_path.iterdir()) == []
    command.assert_not_called()


def test_dangling_manifest_symlink_is_not_followed(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "must-not-be-created"
    requirements = root / "requirements.txt"
    requirements.symlink_to(outside)

    with patch("subprocess.run", side_effect=successful_command) as command:
        with pytest.raises(ProjectInitializationError):
            initialize(root)

    assert requirements.is_symlink()
    assert not outside.exists()
    command.assert_not_called()


def test_poetry_does_not_change_global_configuration(tmp_path: Path) -> None:
    with patch("subprocess.run", side_effect=successful_command) as command:
        initialize(tmp_path, manager="poetry")

    assert all(call.args[0][:2] != ["poetry", "config"] for call in command.call_args_list)
    assert all(call.kwargs["env"]["POETRY_VIRTUALENVS_IN_PROJECT"] == "true" for call in command.call_args_list)


def test_uv_install_explicitly_targets_the_new_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "unrelated-environment"))
    with patch("subprocess.run", side_effect=successful_command) as command:
        initialize(tmp_path, manager="uv")

    installation = next(call.args[0] for call in command.call_args_list if call.args[0][:3] == ["uv", "pip", "install"])
    interpreter = installation[installation.index("--python") + 1]
    assert Path(interpreter).is_relative_to(tmp_path / ".venv")


def test_poetry_ignores_inherited_active_environments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VIRTUAL_ENV", "/unrelated-environment")
    monkeypatch.setenv("CONDA_PREFIX", "/unrelated-conda")
    with patch("subprocess.run", side_effect=successful_command) as command:
        initialize(tmp_path, manager="poetry")

    for call in command.call_args_list:
        assert "VIRTUAL_ENV" not in call.kwargs["env"]
        assert "CONDA_PREFIX" not in call.kwargs["env"]
    assert command.call_args_list[0].args[0][:3] == [sys.executable, "-m", "venv"]


@pytest.mark.parametrize("relative_path", ["requirements.txt", "data/sample_table.csv", ".venv/bin/python"])
def test_rollback_preserves_modified_generated_files(tmp_path: Path, relative_path: str, monkeypatch) -> None:
    class ModifyThenFail:
        name = "modify generated resource"

        def execute(self, context):
            (context.root_path / relative_path).write_text("modified by another actor", encoding="utf-8")
            raise OSError("downstream failure")

        def rollback(self, context):
            pass

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, ModifyThenFail()])
    with patch("subprocess.run", side_effect=successful_command):
        with pytest.raises(ProjectInitializationError, match="downstream failure"):
            initialize(tmp_path)

    assert (tmp_path / relative_path).read_text(encoding="utf-8") == "modified by another actor"


def test_rollback_preserves_new_untracked_environment_files(tmp_path: Path, monkeypatch) -> None:
    class ForeignFileThenFail:
        name = "foreign file"

        def execute(self, context):
            (context.root_path / ".venv" / "foreign.txt").write_text("keep", encoding="utf-8")
            raise OSError("downstream failure")

        def rollback(self, context):
            pass

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, ForeignFileThenFail()])
    with patch("subprocess.run", side_effect=successful_command):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path)

    assert (tmp_path / ".venv" / "foreign.txt").read_text(encoding="utf-8") == "keep"
    assert not (tmp_path / ".venv" / "bin").exists()


def test_replaced_parent_symlink_cannot_redirect_rollback(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sample_table.csv"
    sentinel.write_text("external data", encoding="utf-8")

    class RedirectThenFail:
        name = "redirect parent"

        def execute(self, context):
            (context.root_path / "data").rename(context.root_path / "saved-data")
            (context.root_path / "data").symlink_to(outside, target_is_directory=True)
            raise OSError("downstream failure")

        def rollback(self, context):
            pass

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, RedirectThenFail()])
    with patch("subprocess.run", side_effect=successful_command):
        with pytest.raises(ProjectInitializationError):
            initialize(root)

    assert sentinel.read_text(encoding="utf-8") == "external data"
    assert (root / "data").is_symlink()
    assert (root / "saved-data" / "sample_table.csv").exists()


def test_rollback_removes_owned_symlinks_without_touching_their_targets(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "external-python"
    outside.write_text("external executable", encoding="utf-8")

    def create_then_fail(*arguments, **options):
        environment = options["cwd"] / ".venv"
        (environment / "python").symlink_to(outside)
        raise subprocess.CalledProcessError(1, arguments[0])

    with patch("subprocess.run", side_effect=create_then_fail):
        with pytest.raises(ProjectInitializationError):
            initialize(root)

    assert outside.read_text(encoding="utf-8") == "external executable"
    assert list(root.iterdir()) == []


def test_replaced_environment_tree_is_preserved(tmp_path: Path, monkeypatch) -> None:
    class ReplaceThenFail:
        name = "replace environment"

        def execute(self, context):
            (context.root_path / ".venv").rename(context.root_path / "saved-env")
            (context.root_path / ".venv").mkdir()
            (context.root_path / ".venv" / "foreign.txt").write_text("keep", encoding="utf-8")
            raise OSError("downstream failure")

        def rollback(self, context):
            pass

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, ReplaceThenFail()])
    with patch("subprocess.run", side_effect=successful_command):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path)

    assert (tmp_path / ".venv" / "foreign.txt").read_text(encoding="utf-8") == "keep"
    assert (tmp_path / "saved-env" / "bin" / "python").exists()


@pytest.mark.parametrize("manager", ["venv", "uv", "poetry"])
def test_real_environment_creation_can_be_rolled_back_without_installing_packages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, manager: str
) -> None:
    if manager == "uv" and shutil.which("uv") is None:
        pytest.skip("uv executable is not installed")
    monkeypatch.setenv("UV_OFFLINE", "true")
    monkeypatch.setenv("UV_PYTHON_DOWNLOADS", "never")
    monkeypatch.setenv("UV_PYTHON", sys.executable)
    strategy_type = type(EnvironmentRegistry.resolve(manager))

    class FailAfterEnvironment:
        name = "failure after native environment creation"

        def execute(self, context):
            interpreter = (
                context.root_path / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
            )
            assert interpreter.exists()
            raise OSError("native environment failure fixture")

        def rollback(self, context):
            pass

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, FailAfterEnvironment()])
    with patch.object(strategy_type, "install_dependencies"):
        with pytest.raises(ProjectInitializationError, match="native environment failure fixture"):
            initialize(tmp_path, manager=manager)

    assert list(tmp_path.iterdir()) == []


def test_partial_file_write_is_journaled_before_failure(tmp_path: Path) -> None:
    context = ScaffoldContext(tmp_path, "safety", "0.1.0", "Safety", "Tester")
    original_open = Path.open

    class PartialStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *arguments):
            return self.stream.__exit__(*arguments)

        def fileno(self):
            return self.stream.fileno()

        def write(self, content):
            self.stream.write(content[:5])
            self.stream.flush()
            raise OSError("partial write failure")

    def partial_open(path, mode="r", *arguments, **options):
        stream = original_open(path, mode, *arguments, **options)
        return PartialStream(stream) if mode == "x" else stream

    with patch.object(Path, "open", new=partial_open):
        with pytest.raises(OSError, match="partial write failure"):
            context.write_text(tmp_path / "partial.txt", "unfinished content")

    context.rollback_created(final=True)
    assert list(tmp_path.iterdir()) == []


def test_poetry_install_failure_cleans_new_lock_and_preserves_existing_requirements(tmp_path: Path) -> None:
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("unrelated requirement", encoding="utf-8")

    def fail_install(*arguments, **options):
        if arguments[0][:2] == ["poetry", "install"]:
            (options["cwd"] / "poetry.lock").write_text("partial lock", encoding="utf-8")
            raise subprocess.CalledProcessError(1, arguments[0], stderr="install failed")
        return successful_command(*arguments, **options)

    with patch("subprocess.run", side_effect=fail_install):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path, manager="poetry")

    assert list(tmp_path.iterdir()) == [requirements]
    assert requirements.read_text(encoding="utf-8") == "unrelated requirement"


def test_cleanup_continues_after_a_custom_rollback_fails(tmp_path: Path, monkeypatch) -> None:
    class BrokenHook:
        name = "broken custom hook"

        def execute(self, context):
            context.write_text(context.root_path / "hook.txt", "owned hook artifact")
            raise OSError("original failure")

        def rollback(self, context):
            raise OSError("rollback failure")

    monkeypatch.setattr(ProjectInitializer, "_pipeline", [*ProjectInitializer._pipeline, BrokenHook()])
    with patch("subprocess.run", side_effect=successful_command):
        with pytest.raises(ProjectInitializationError, match="original failure") as failure:
            initialize(tmp_path)

    assert str(failure.value.__cause__) == "original failure"
    assert list(tmp_path.iterdir()) == []


def test_manifest_created_after_preflight_is_preserved(tmp_path: Path) -> None:
    original_execute = WorkspaceEnvironmentStep.execute

    def create_competing_manifest(step, context):
        (context.root_path / "requirements.txt").write_text("another creator", encoding="utf-8")
        original_execute(step, context)

    with (
        patch("subprocess.run", side_effect=successful_command),
        patch.object(WorkspaceEnvironmentStep, "execute", new=create_competing_manifest),
    ):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path)

    assert (tmp_path / "requirements.txt").read_text(encoding="utf-8") == "another creator"
    assert list(tmp_path.iterdir()) == [tmp_path / "requirements.txt"]


def test_unrelated_files_in_existing_directories_survive_failure(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    existing = data / "existing.csv"
    existing.write_text("unrelated data", encoding="utf-8")

    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "creation")):
        with pytest.raises(ProjectInitializationError):
            initialize(tmp_path)

    assert list(tmp_path.iterdir()) == [data]
    assert list(data.iterdir()) == [existing]
    assert existing.read_text(encoding="utf-8") == "unrelated data"
