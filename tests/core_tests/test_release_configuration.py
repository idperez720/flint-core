"""Exercise the release configuration without publishing or changing project history."""

import re
import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from git import Actor, Repo
from semantic_release.cli.commands.main import main
from semantic_release.cli.config import RawConfig

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def release_repository(tmp_path, monkeypatch):
    configuration = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    current = tomllib.loads(configuration)["project"]["version"]
    (tmp_path / "pyproject.toml").write_text(
        configuration.replace(f'version = "{current}"', 'version = "0.5.0"', 1), encoding="utf-8"
    )
    package = tmp_path / "flint_core"
    package.mkdir()
    (package / "__init__.py").write_text('__version__ = "0.5.0"\n', encoding="utf-8")
    repository = Repo.init(tmp_path, initial_branch="main")
    repository.create_remote("origin", "https://github.com/idperez720/flint-core.git")
    identity = Actor("Release Test", "release-test@example.invalid")
    repository.index.add(["pyproject.toml", "flint_core/__init__.py"])
    repository.index.commit("chore: baseline", author=identity, committer=identity)
    repository.create_tag("v0.5.0")
    monkeypatch.chdir(tmp_path)
    with patch("requests.sessions.Session.request", side_effect=AssertionError("Release checks must remain offline.")):
        yield repository, identity
    repository.close()


def add_change(repository, identity, message):
    path = Path(repository.working_tree_dir) / "change.txt"
    path.write_text(message, encoding="utf-8")
    repository.index.add(["change.txt"])
    repository.index.commit(message, author=identity, committer=identity)


def test_release_configuration_uses_supported_keys_and_updates_both_versions():
    release = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["semantic_release"]
    assert not set(release) - RawConfig.model_fields.keys()
    configuration = RawConfig.model_validate(release)
    assert configuration.version_variables == ("flint_core/__init__.py:__version__",)
    assert configuration.version_toml == ("pyproject.toml:project.version",)
    assert configuration.allow_zero_version is True
    assert configuration.major_on_zero is False
    assert set(configuration.branches) == {"main"}
    assert re.fullmatch(configuration.branches["main"].match, "main")
    for branch in ("master", "dev", "codex/write-safety", "main-preview"):
        assert re.match(configuration.branches["main"].match, branch) is None


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("fix: preserve an existing table", "0.5.1"),
        ("feat: support local Spark execution", "0.6.0"),
        ("refactor!: remove deferred utilities", "0.6.0"),
    ],
)
def test_release_prediction_does_not_mutate_files_or_history(release_repository, message, expected):
    repository, identity = release_repository
    add_change(repository, identity, message)
    before = repository.head.commit.hexsha
    response = CliRunner().invoke(main, ["--noop", "version", "--print"])
    assert response.exit_code == 0, response.output
    assert expected in response.output.splitlines()
    assert repository.head.commit.hexsha == before
    assert [tag.name for tag in repository.tags] == ["v0.5.0"]
    assert not repository.is_dirty(untracked_files=True)


def test_local_version_application_keeps_metadata_in_sync_without_commit_or_tag(release_repository):
    repository, identity = release_repository
    add_change(repository, identity, "refactor!: remove deferred utilities")
    before = repository.head.commit.hexsha
    response = CliRunner().invoke(
        main,
        ["version", "--no-commit", "--no-tag", "--no-push", "--no-vcs-release", "--no-changelog", "--skip-build"],
    )
    assert response.exit_code == 0, response.output
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    assert metadata["project"]["version"] == "0.6.0"
    assert Path("flint_core/__init__.py").read_text(encoding="utf-8").strip() == '__version__ = "0.6.0"'
    assert repository.head.commit.hexsha == before
    assert [tag.name for tag in repository.tags] == ["v0.5.0"]
    assert not Path("dist").exists()
    assert not Path("CHANGELOG.md").exists()


def test_feature_branch_cannot_release(release_repository):
    repository, identity = release_repository
    add_change(repository, identity, "feat: support local Spark execution")
    repository.git.checkout("-b", "codex/not-a-release")
    before = repository.head.commit.hexsha
    response = CliRunner().invoke(main, ["--strict", "--noop", "version", "--print"])
    assert response.exit_code != 0
    assert "0.6.0" not in response.output.splitlines()
    assert repository.head.commit.hexsha == before
    assert [tag.name for tag in repository.tags] == ["v0.5.0"]
    assert not repository.is_dirty(untracked_files=True)
