"""Unit tests for the flint-core transactional project scaffolding engine."""

from pathlib import Path
from typing import List

import pytest
from click.testing import CliRunner

from flint_core.cli import init
from flint_core.core.initializer import (
    ProjectInitializationError,
    ProjectInitializer,
    ScaffoldContext,
)


def test_project_initializer_happy_path(tmp_path: Path) -> None:
    """Asserts that a standard project initialization seeds all assets."""
    initializer = ProjectInitializer(base_path=tmp_path)

    initializer.init_project(
        name="enterprise-pipeline",
        version="1.0.0",
        description="Core financial ledger ingestion architecture.",
        author="Architecture Team",
        envs=["dev", "qa", "prod"],
        pattern="default",
    )

    # 1. Verify directory structural layout
    assert (tmp_path / "conf" / "catalog").is_dir()
    assert (tmp_path / "src" / "notebooks").is_dir()
    assert (tmp_path / "data").is_dir()

    # 2. Verify pyproject.toml manifest content
    toml_path = tmp_path / "pyproject.toml"
    assert toml_path.is_file()
    toml_content = toml_path.read_text(encoding="utf-8")
    assert 'name = "enterprise-pipeline"' in toml_content
    assert 'version = "1.0.0"' in toml_content
    assert 'name = "Architecture Team"' in toml_content

    # 3. Verify boilerplate data assets (Updated for Task 2 specs)
    assert (tmp_path / "data" / "sample_table.csv").is_file()
    assert (tmp_path / "conf" / "catalog" / "datasets.yaml").is_file()

    # 4. Verify Phase 3.5 multi-environment isolated assets
    assert (tmp_path / "conf" / "envs" / "dev" / "variables.yml").is_file()
    assert (tmp_path / "conf" / "envs" / "dev" / "spark.yml").is_file()


def test_project_initializer_medallion_pattern(tmp_path: Path) -> None:
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
    assert (tmp_path / "conf" / "catalog" / "bronze" / "sample_dataset.yaml").is_file()


def test_project_initializer_datamesh_pattern(tmp_path: Path) -> None:
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
    assert (tmp_path / "conf" / "catalog" / "finance" / "sample_contracts.yaml").is_file()


def test_cli_init_interactive_numeric_choice(tmp_path: Path) -> None:
    """Asserts that CLI numeric choices map to correct catalog paradigms."""
    runner = CliRunner()
    # Sequence matching the click prompts: index choice 2 maps to medallion
    inputs: List[str] = [
        "cli-medallion-project",  # Project name
        "0.2.0",  # Version
        "CLI numeric test",  # Description
        "CLI Tester",  # Author
        "dev,prod",  # Environments
        "2",  # Pattern Choice (2 = medallion)
    ]

    result = runner.invoke(
        init,
        ["--path", str(tmp_path)],
        input="\n".join(inputs) + "\n",
    )

    assert result.exit_code == 0
    assert "Project successfully initialized" in result.output
    assert (tmp_path / "conf" / "catalog" / "bronze").is_dir()
    assert (tmp_path / "conf" / "envs" / "prod" / "variables.yml").is_file()


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
    assert "dont-overwrite-me" in existing_toml.read_text(encoding="utf-8")


def test_project_initializer_custom_step_registration(tmp_path: Path) -> None:
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


def test_project_initializer_transactional_rollback(tmp_path: Path) -> None:
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
