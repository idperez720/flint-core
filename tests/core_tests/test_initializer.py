"""Unit tests for the flint-core transactional project scaffolding engine."""

import subprocess
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from flint_core.cli import init
from flint_core.core.initializer import (
    ProjectInitializationError,
    ProjectInitializer,
    ScaffoldContext,
)


def mock_subprocess_run(*args: object, **kwargs: object) -> MagicMock:
    """Simulates a fast and successful virtual environment directory creation.

    Args:
        *args: Variable positional arguments matching subprocess signatures.
        **kwargs: Variable keyword arguments containing execution paths.

    Returns:
        A mock instance mimicking a successful subprocess execution return.
    """
    cwd = kwargs.get("cwd")
    if isinstance(cwd, Path):
        venv_dir: Path = cwd / ".venv"
        venv_dir.mkdir(exist_ok=True)
    return MagicMock()


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_project_initializer_happy_path(mock_run: MagicMock, tmp_path: Path) -> None:
    """Asserts that a standard project initialization seeds all assets."""
    initializer = ProjectInitializer(base_path=tmp_path)

    initializer.init_project(
        name="enterprise-pipeline",
        version="1.0.0",
        description="Core financial ledger ingestion architecture.",
        author="Architecture Team",
        envs=["dev", "qa", "prod"],
        pattern="default",
        manager="venv",
    )

    # 1. Verify directory structural layout
    assert (tmp_path / "conf" / "catalog").is_dir()
    assert (tmp_path / "src" / "notebooks").is_dir()
    assert (tmp_path / "data").is_dir()

    # 2. Verify hybrid manifest and requirements contents
    toml_path = tmp_path / "pyproject.toml"
    assert toml_path.is_file()
    toml_content = toml_path.read_text(encoding="utf-8")
    assert 'name = "enterprise-pipeline"' in toml_content

    req_path = tmp_path / "requirements.txt"
    assert req_path.is_file()
    req_content = req_path.read_text(encoding="utf-8")
    assert "flint-core>=" in req_content

    # 3. Verify automated virtual environment isolation anchor
    assert (tmp_path / ".venv").is_dir()

    # 4. Verify boilerplate data assets
    assert (tmp_path / "data" / "sample_table.csv").is_file()
    assert (tmp_path / "conf" / "catalog" / "datasets.yaml").is_file()


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_project_initializer_medallion_pattern(mock_run: MagicMock, tmp_path: Path) -> None:
    """Asserts that medallion pattern builds bronze, silver, and gold branches."""
    initializer = ProjectInitializer(base_path=tmp_path)

    initializer.init_project(
        name="medallion-pipeline",
        version="0.1.0",
        description="Medallion lakehouse architecture.",
        author="Data Platform Team",
        envs=["dev"],
        pattern="medallion",
    )

    assert (tmp_path / "conf" / "catalog" / "bronze").is_dir()
    assert (tmp_path / "conf" / "catalog" / "silver").is_dir()
    assert (tmp_path / "conf" / "catalog" / "gold").is_dir()


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_project_initializer_datamesh_pattern(mock_run: MagicMock, tmp_path: Path) -> None:
    """Asserts domain-driven layouts allocate distinct custom directories."""
    initializer = ProjectInitializer(base_path=tmp_path)

    initializer.init_project(
        name="mesh-pipeline",
        version="1.0.0",
        description="Data mesh architecture.",
        author="Mesh Team",
        envs=["dev"],
        pattern="datamesh",
        domains=["finance", "marketing"],
    )

    assert (tmp_path / "conf" / "catalog" / "finance").is_dir()
    assert (tmp_path / "conf" / "catalog" / "marketing").is_dir()


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_cli_init_interactive_numeric_choice(mock_run: MagicMock, tmp_path: Path) -> None:
    """Asserts that CLI inputs including manager map to correct paradigms."""
    runner = CliRunner()
    inputs: List[str] = [
        "cli-medallion-project",  # Project name
        "0.2.0",  # Version
        "CLI numeric test",  # Description
        "CLI Tester",  # Author
        "dev,prod",  # Environments
        "2",  # Pattern Choice (2 = medallion)
        "venv",  # NEW: Package manager selection prompt items
    ]

    result = runner.invoke(
        init,
        ["--path", str(tmp_path)],
        input="\n".join(inputs) + "\n",
    )

    assert result.exit_code == 0
    assert "Project successfully initialized" in result.output
    assert (tmp_path / "conf" / "catalog" / "bronze").is_dir()
    assert (tmp_path / ".venv").is_dir()


def test_project_initializer_collision_raises_exception(tmp_path: Path) -> None:
    """Asserts that initializing over an existing manifest safely aborts."""
    existing_toml = tmp_path / "pyproject.toml"
    existing_toml.write_text("[project]\nname = 'dont-overwrite-me'", encoding="utf-8")

    initializer = ProjectInitializer(base_path=tmp_path)

    with pytest.raises(ProjectInitializationError) as exc_info:
        initializer.init_project(
            name="collision-test",
            version="0.1.0",
            description="Should fail",
            author="Tester",
        )

    assert "scaffolding transaction failed" in str(exc_info.value)


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_project_initializer_custom_step_registration(mock_run: MagicMock, tmp_path: Path) -> None:
    """Validates framework capabilities to inject custom workflow steps."""

    class MockGitInitStep:
        @property
        def name(self) -> str:
            return "Mock Git Repository Initialization"

        def execute(self, context: ScaffoldContext) -> None:
            git_dir = context.root_path / ".git"
            git_dir.mkdir()
            context.created_paths.append(git_dir)

        def rollback(self, context: ScaffoldContext) -> None:
            git_dir = context.root_path / ".git"
            if git_dir.exists():
                git_dir.rmdir()

    original_pipeline = ProjectInitializer._pipeline[:]

    try:
        ProjectInitializer.register_scaffold_step(MockGitInitStep())
        initializer = ProjectInitializer(base_path=tmp_path)

        initializer.init_project(
            name="custom-step-test",
            version="0.1.0",
            description="Testing IoC hooks",
            author="Tester",
        )

        assert (tmp_path / ".git").is_dir()
        assert (tmp_path / "pyproject.toml").is_file()

    finally:
        ProjectInitializer._pipeline = original_pipeline


@patch("subprocess.run", side_effect=mock_subprocess_run)
def test_project_initializer_transactional_rollback(mock_run: MagicMock, tmp_path: Path) -> None:
    """Asserts atomicity: failures trigger full reverse cleanups cleanly."""

    class CatastrophicFaultyStep:
        @property
        def name(self) -> str:
            return "Simulated Network/Disk Failure Hook"

        def execute(self, context: ScaffoldContext) -> None:
            raise IOError("No space left on device or permission denied.")

        def rollback(self, context: ScaffoldContext) -> None:
            pass

    original_pipeline = ProjectInitializer._pipeline[:]

    try:
        ProjectInitializer.register_scaffold_step(CatastrophicFaultyStep())
        initializer = ProjectInitializer(base_path=tmp_path)

        with pytest.raises(ProjectInitializationError):
            initializer.init_project(
                name="broken-transaction",
                version="1.0.0",
                description="Rollback verification",
                author="Tester",
            )

        # The directory must be completely pristine due to transaction rollback
        remaining_files = [p for p in tmp_path.rglob("*") if p != tmp_path]
        assert len(remaining_files) == 0

    finally:
        ProjectInitializer._pipeline = original_pipeline
