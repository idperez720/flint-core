"""Python/CLI equivalence, isolation, and honest partial failure reporting."""

import copy
import json
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy
from flint_core.cli import entry_point
from flint_core.runtime.errors import PlanError
from flint_core.runtime.local_csv import LocalCsvAdapter

from .test_project import make_project

FACTORY = """from flint_core import Node, Output, Pipeline, WritePolicy
def transform(source):
    return source.assign(id=source["id"] * 2)
def build():
    return [Pipeline("main", [Node("double", transform, {"source": "source"}, Output("result", WritePolicy()))])]
"""


def executable(root):
    make_project(root, factory=FACTORY)
    (root / "data").mkdir(exist_ok=True)
    (root / "data" / "source.csv").write_text("id\n1\n2\n", encoding="utf-8")
    return FlintProject.load(root)


def test_python_and_cli_run_identical_workflows(tmp_path):
    project = executable(tmp_path / "python")
    result = project.run()
    cli_project = executable(tmp_path / "cli")
    response = CliRunner().invoke(entry_point, ["run", "--project", str(cli_project.root), "--json"])
    summary = json.loads(response.output)
    assert response.exit_code == 0
    assert result.success and summary["status"] == "success"
    assert result.completed_nodes == tuple(summary["completed_nodes"])
    assert (project.root / "data/result.csv").read_bytes() == (cli_project.root / "data/result.csv").read_bytes()
    assert LocalCsvAdapter().read(project.catalog.snapshot, "result")["id"].tolist() == [2, 4]


def test_second_run_preserves_target_and_returns_failure(tmp_path):
    project = executable(tmp_path)
    assert project.run().success
    path = tmp_path / "data/result.csv"
    previous = path.read_bytes()
    result = project.run()
    assert result.status == "failed"
    assert result.publications == ()
    assert path.read_bytes() == previous


def test_dry_run_has_no_dataset_reads_and_validate_does_not_import_factory(tmp_path):
    root = make_project(tmp_path, factory=FACTORY)
    response = CliRunner().invoke(entry_point, ["run", "--project", str(root), "--dry-run", "--json"])
    assert response.exit_code == 0
    assert json.loads(response.output)["status"] == "planned"
    assert not (root / "data").exists()
    (root / "src/project_code/registry.py").write_text('raise Exception("not imported")', encoding="utf-8")
    response = CliRunner().invoke(entry_point, ["validate", "--project", str(root), "--json"])
    assert response.exit_code == 0


def test_foreign_stale_and_forged_plans_are_rejected(tmp_path):
    project = executable(tmp_path / "first")
    other = executable(tmp_path / "second")
    plan = project.compile()
    with pytest.raises(PlanError):
        other.run(plan)
    with pytest.raises(PlanError):
        project.run(replace(plan))
    project.catalog.reload()
    with pytest.raises(PlanError):
        project.run(plan)
    assert not (project.root / "data/result.csv").exists()


def test_uncertain_commit_is_reported_without_retry(tmp_path):
    project = executable(tmp_path)
    with patch.object(LocalCsvAdapter, "_sync_parent", side_effect=OSError("secret backend error")) as sync:
        result = project.run()
    assert result.status == "uncertain"
    assert result.publications[0].state == "uncertain"
    assert sync.call_count == 1
    assert "secret" not in json.dumps(result.to_dict())
    assert (tmp_path / "data/result.csv").exists()


def test_partial_publication_is_not_described_as_rollback(tmp_path):
    project = executable(tmp_path)
    definitions = yaml.safe_load((tmp_path / "definitions.yaml").read_text())
    definitions["datasets"]["later"] = copy.deepcopy(definitions["datasets"]["result"])
    (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()

    def crash(source):
        raise RuntimeError("private connection credential")

    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node("publish", lambda source: source, {"source": "source"}, Output("result", WritePolicy())),
                    Node("fail", crash, {"source": "result"}, Output("later")),
                ],
            )
        }
    )
    result = project.run()
    assert result.status == "failed"
    assert result.completed_nodes == ("publish",)
    assert result.publications[0].state == "committed"
    assert result.failed_node == "fail"
    assert (tmp_path / "data/result.csv").exists()
    assert "credential" not in json.dumps(result.to_dict())


def test_consumer_mutation_does_not_change_other_consumer_input(tmp_path):
    project = executable(tmp_path)
    seen = []

    def inspect(left, right):
        seen.append(left["id"].tolist())
        left.loc[0, "id"] = 99
        seen.append(right["id"].tolist())
        return right

    project._pipelines = MappingProxyType(
        {"main": Pipeline("main", [Node("inspect", inspect, {"left": "source", "right": "source"}, Output("result"))])}
    )
    result = project.run()
    assert result.success
    assert seen == [[1, 2], [1, 2]]
    assert not (tmp_path / "data/result.csv").exists()


def test_bad_input_fails_before_transform(tmp_path):
    project = executable(tmp_path)
    (tmp_path / "data/source.csv").write_text("id\nnot-an-integer\n", encoding="utf-8")
    with patch.object(LocalCsvAdapter, "publish") as publish:
        result = project.run()
    assert result.error_code == "data_validation_failed"
    publish.assert_not_called()


def test_json_suppresses_trusted_factory_and_function_prints(tmp_path):
    factory = FACTORY.replace("def transform(source):", 'print("private-factory-value")\ndef transform(source):')
    factory = factory.replace("return source.assign", 'print("private-row-value")\n    return source.assign')
    make_project(tmp_path, factory=factory)
    (tmp_path / "data").mkdir()
    (tmp_path / "data/source.csv").write_text("id\n1\n", encoding="utf-8")
    response = CliRunner().invoke(entry_point, ["run", "--project", str(tmp_path), "--json"])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["status"] == "success"
    assert "private" not in response.output


def test_cli_uncertain_exit_code_is_distinct(tmp_path):
    executable(tmp_path)
    with patch.object(LocalCsvAdapter, "_sync_parent", side_effect=OSError("unknown durability")):
        response = CliRunner().invoke(entry_point, ["run", "--project", str(tmp_path), "--json"])
    assert response.exit_code == 3
    assert json.loads(response.output)["status"] == "uncertain"


def test_unbound_intermediate_is_used_without_disk_publication(tmp_path):
    project = executable(tmp_path)
    definitions = yaml.safe_load((tmp_path / "definitions.yaml").read_text())
    definitions["datasets"]["intermediate"] = copy.deepcopy(definitions["datasets"]["result"])
    (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node("first", lambda source: source, {"source": "source"}, Output("intermediate")),
                    Node("second", lambda source: source, {"source": "intermediate"}, Output("result", WritePolicy())),
                ],
            )
        }
    )
    read = LocalCsvAdapter.read
    reads = []

    def observe(adapter, snapshot, dataset):
        reads.append(dataset)
        return read(adapter, snapshot, dataset)

    with patch.object(LocalCsvAdapter, "read", observe):
        result = project.run()
    assert result.success
    assert reads == ["source"]
    assert [item.dataset for item in result.publications] == ["result"]


def test_uncertain_cleanup_is_reported_in_summary(tmp_path):
    project = executable(tmp_path)
    with (
        patch.object(LocalCsvAdapter, "_sync_parent", side_effect=OSError("unknown durability")),
        patch.object(LocalCsvAdapter, "_cleanup", return_value=True),
    ):
        result = project.run()
    assert result.status == "uncertain"
    assert result.cleanup_pending
    assert result.to_dict()["cleanup_pending"]
