"""SAFE/CORE evidence for explicit Spark and Pandas Delta execution in v1."""

import copy
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import MappingProxyType
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy
from flint_core.cli import entry_point
from flint_core.core.exceptions import CatalogParseError
from flint_core.runtime import directory
from flint_core.runtime.errors import (
    DataValidationError,
    PlanError,
    ProjectError,
    PublicationConflict,
    PublicationUncertain,
)
from flint_core.runtime.local_delta import PandasDeltaAdapter

from .test_project import make_project


def project_files(root, engine="spark", *, factory=None):
    factory = (
        factory
        or f'''from flint_core import Node, Output, Pipeline, WritePolicy
def identity(source):
    return source
def build():
    return [Pipeline("main", [Node("copy", identity, {{"source": "source"}},
        Output("result", WritePolicy()))], engine="{engine}")]
'''
    )
    make_project(root, factory=factory)
    binding_path = root / "bindings.yaml"
    bindings = yaml.safe_load(binding_path.read_text())
    for name, binding in bindings["bindings"].items():
        binding.update(
            adapter=engine + ".delta", format="delta", target={"kind": "delta_table", "path": "data/" + name}
        )
    binding_path.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    (root / "data").mkdir()
    return root


def load_project(root, engine="spark", **options):
    return FlintProject.load(project_files(root, engine, **options))


@pytest.fixture(scope="module")
def session(request):
    if not request.config.getoption("--spark-delta"):
        pytest.skip("Real v1 Spark Delta checks require --spark-delta.")
    from flint_core.runtime.spark import open_session

    spark, owned = open_session(local=True)
    assert owned
    yield spark
    spark.stop()


def seed(project, session=None):
    target = project.root / "data/source"
    if session is not None:
        session.range(2).write.format("delta").mode("error").save(str(target))
    else:
        deltalake = pytest.importorskip("deltalake")
        arrow = pytest.importorskip("pyarrow")
        deltalake.write_deltalake(target, arrow.table({"id": [0, 1]}))


def test_metadata_and_dry_run_need_no_engines_or_targets(tmp_path):
    root = project_files(tmp_path)
    script = """import builtins, json
original = builtins.__import__
def blocked(name, *args, **kwargs):
    if name.split(".")[0] in {"pandas", "pyspark", "delta", "deltalake", "pyarrow"}:
        raise ImportError("engine deliberately unavailable")
    return original(name, *args, **kwargs)
builtins.__import__ = blocked
from flint_core import FlintProject
with FlintProject.load(ROOT) as project:
    assert project.compile().engine == "spark"
    print(json.dumps(project.compile().to_dict()))
""".replace("ROOT", repr(str(root)))
    completed = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    assert json.loads(completed.stdout)["engine"] == "spark"
    assert list((root / "data").iterdir()) == []
    response = CliRunner().invoke(entry_point, ["run", "--project", str(root), "--dry-run", "--spark-local", "--json"])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["engine"] == "spark"


@pytest.mark.parametrize("engine", ["spark", "pandas"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("target", {"kind": "file", "path": "data/table"}),
        ("format", "csv"),
        ("options", {"mode": "overwrite"}),
        ("options", {"version": True}),
        ("options", {"version": -1}),
        ("options", {"version": "0"}),
        ("options", {"timestampAsOf": "2026-10-18"}),
    ],
)
def test_delta_metadata_rejects_unenforced_or_mismatched_fields(tmp_path, engine, field, value):
    root = project_files(tmp_path, engine)
    manifest = root / "bindings.yaml"
    bindings = yaml.safe_load(manifest.read_text())
    bindings["bindings"]["source"][field] = value
    manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    with pytest.raises(ProjectError) as caught:
        FlintProject.load(root)
    assert isinstance(caught.value.__cause__, (type(None), CatalogParseError))
    assert list((root / "data").iterdir()) == []


def test_cross_engine_binding_and_unsupported_policy_fail_before_io(tmp_path):
    project = load_project(tmp_path)
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main", [Node("copy", lambda source: source, {"source": "source"}, Output("result", WritePolicy()))]
            )
        }
    )
    with pytest.raises(PlanError):
        project.compile()
    for mode in ("append", "overwrite", "ignore", "error"):
        project._pipelines = MappingProxyType(
            {
                "main": Pipeline(
                    "main",
                    [Node("produce", lambda: None, {}, Output("result", WritePolicy(mode=mode)))],
                    engine="spark",
                )
            }
        )
        with pytest.raises(PlanError):
            project.compile()


def test_directory_publication_exclusivity_and_uncertain_outcomes(tmp_path):
    project = load_project(tmp_path, "pandas")
    snapshot = project.catalog.snapshot
    publisher = directory.DirectoryPublisher()
    target = tmp_path / "data/result"

    def prepare(stage):
        (stage / "owned").write_bytes(b"complete")

    real_rename = directory.rename_exclusive

    def competing(stage, destination):
        destination.mkdir()
        (destination / "foreign").write_bytes(b"preserve")
        real_rename(stage, destination)

    with patch.object(directory, "rename_exclusive", competing):
        with pytest.raises(PublicationConflict):
            publisher.publish(snapshot, "result", prepare)
    assert (target / "foreign").read_bytes() == b"preserve"
    assert not list(target.parent.glob(".flint-delta-*"))
    (target / "foreign").unlink()
    target.rmdir()

    def reported_failure(stage, destination):
        real_rename(stage, destination)
        raise OSError("unknown commit response")

    with patch.object(directory, "rename_exclusive", reported_failure):
        with pytest.raises(PublicationUncertain):
            publisher.publish(snapshot, "result", prepare)
    assert (target / "owned").read_bytes() == b"complete"


def test_directory_preparation_failure_cleans_only_owned_stage(tmp_path):
    project = load_project(tmp_path, "pandas")

    def broken(stage):
        (stage / "partial").write_bytes(b"partial")
        raise OSError("preparation failed")

    with pytest.raises(DataValidationError):
        directory.DirectoryPublisher().publish(project.catalog.snapshot, "result", broken)
    assert list((tmp_path / "data").iterdir()) == []


def test_pandas_delta_runtime_and_pinned_snapshot(tmp_path):
    pandas = pytest.importorskip("pandas")
    deltalake = pytest.importorskip("deltalake")
    arrow = pytest.importorskip("pyarrow")
    project = load_project(tmp_path, "pandas")
    seed(project)
    deltalake.write_deltalake(tmp_path / "data/source", arrow.table({"id": [2]}), mode="append")
    binding_path = tmp_path / "bindings.yaml"
    bindings = yaml.safe_load(binding_path.read_text())
    bindings["bindings"]["source"]["options"] = {"version": 0}
    binding_path.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    project.catalog.reload()
    result = project.run()
    assert result.success, result.to_dict()
    actual = PandasDeltaAdapter().read(project.catalog.snapshot, "result")
    pandas.testing.assert_series_equal(actual["id"], pandas.Series([0, 1], dtype="Int64", name="id"), check_index=False)
    previous = {path.name: path.read_bytes() for path in (tmp_path / "data/result/_delta_log").glob("*.json")}
    assert project.run().status == "failed"
    assert previous == {path.name: path.read_bytes() for path in (tmp_path / "data/result/_delta_log").glob("*.json")}


def test_spark_delta_runtime_borrows_session_and_releases_owned_checkpoints(tmp_path, session):
    project = load_project(tmp_path)
    seed(project, session)
    caller_cache = session.range(4).cache()
    caller_cache.count()
    from flint_core.runtime.spark import SparkFrames

    validate = SparkFrames.validate
    checkpoints = set()
    owners = []

    def observed(owner, frame, definition):
        owned = validate(owner, frame, definition)
        checkpoints.add(owned._jdf.logicalPlan().rdd().id())
        owners.append(owner)
        return owned

    try:
        with patch.object(SparkFrames, "validate", observed):
            result = project.run(spark=session)
        assert result.success, result.to_dict()
        assert [item.state for item in result.publications] == ["committed"]
        assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 2
        retained = set(session.sparkContext._jsc.getPersistentRDDs().keys())
        assert not checkpoints & retained
        assert all(not owner._owned for owner in owners)
        assert caller_cache.is_cached
        assert project.run(spark=session).status == "failed"
        assert session.range(1).count() == 1
    finally:
        caller_cache.unpersist()


def test_spark_resource_injection_and_session_choice_are_explicit(tmp_path, session):
    project = load_project(tmp_path)
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node(
                        "produce",
                        lambda supplied: supplied.range(2),
                        {},
                        Output("result", WritePolicy()),
                        resources={"supplied": "spark"},
                    )
                ],
                engine="spark",
            )
        }
    )
    assert project.run().status == "failed"
    assert project.run(spark_local=True).status == "failed"
    assert project.run(spark=session, spark_local=True).status == "failed"
    assert project.run(spark=session).success
    assert session.range(1).count() == 1


def test_spark_invalid_output_contracts_fail_before_publication(tmp_path, session):
    from pyspark.sql import functions

    project = load_project(tmp_path)
    manifest = tmp_path / "definitions.yaml"
    definitions = yaml.safe_load(manifest.read_text())
    definitions["datasets"]["result"]["constraints"] = [
        {"id": "unique_id", "type": "unique", "columns": ["id"], "severity": "error"}
    ]
    manifest.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()
    cases = [
        session.range(2).select(functions.col("id").cast("int")),
        session.range(2).select(functions.col("id").cast("string")),
        session.createDataFrame([(None,)], "id long"),
        session.createDataFrame([(1,), (1,)], "id long"),
        session.range(2).withColumn("extra", functions.lit("unexpected")),
        session.range(2).select(functions.col("id").alias("wrong")),
        session.range(2).select("id", "id"),
        None,
    ]
    for bad_frame in cases:
        project._pipelines = MappingProxyType(
            {
                "main": Pipeline(
                    "main",
                    [Node("invalid", lambda value=bad_frame: value, {}, Output("result", WritePolicy()))],
                    engine="spark",
                )
            }
        )
        result = project.run(spark=session)
        assert result.error_code == "data_validation_failed", result.to_dict()
        assert result.publications == ()
        assert not (tmp_path / "data/result").exists()
    assert not list((tmp_path / "data").glob(".flint-delta-*"))


def test_spark_full_typed_contract_round_trip_and_special_column_names(tmp_path, session):
    project = load_project(tmp_path)
    manifest = tmp_path / "definitions.yaml"
    definitions = yaml.safe_load(manifest.read_text())
    schema = {
        "extra_columns": "allow",
        "columns": {
            "count": {"type": "int64", "nullable": False},
            "active": {"type": "boolean", "nullable": True},
            "score": {"type": "float64", "nullable": True},
            "text.value": {"type": "string", "nullable": True},
        },
    }
    for definition in definitions["datasets"].values():
        definition["schema"] = copy.deepcopy(schema)
        definition["constraints"] = [
            {"id": "unique_count", "type": "unique", "columns": ["count"], "severity": "error"}
        ]
    manifest.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()
    values = [(1, True, 1.5, "NA", "extra"), (2, None, None, None, None)]
    dataframe = session.createDataFrame(
        values, "count long, active boolean, score double, `text.value` string, extra string"
    )
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main", [Node("produce", lambda: dataframe, {}, Output("result", WritePolicy()))], engine="spark"
            )
        }
    )
    assert project.run(spark=session).success
    actual = session.read.format("delta").load(str(tmp_path / "data/result"))
    assert sorted(tuple(row) for row in actual.collect()) == values


@pytest.mark.parametrize("invalid", [float("inf"), float("-inf"), float("nan")])
def test_spark_finiteness_and_non_nullable_nan_are_enforced(tmp_path, session, invalid):
    project = load_project(tmp_path)
    manifest = tmp_path / "definitions.yaml"
    definitions = yaml.safe_load(manifest.read_text())
    definitions["datasets"]["result"]["schema"]["columns"] = {"score": {"type": "float64", "nullable": False}}
    manifest.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()
    dataframe = session.createDataFrame([(invalid,)], "score double")
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main", [Node("invalid", lambda: dataframe, {}, Output("result", WritePolicy()))], engine="spark"
            )
        }
    )
    assert project.run(spark=session).error_code == "data_validation_failed"
    assert not (tmp_path / "data/result").exists()


def test_spark_pinned_historical_input_and_bad_version_never_fall_back(tmp_path, session):
    project = load_project(tmp_path)
    seed(project, session)
    session.range(2, 3).write.format("delta").mode("append").save(str(tmp_path / "data/source"))
    manifest = tmp_path / "bindings.yaml"
    bindings = yaml.safe_load(manifest.read_text())
    bindings["bindings"]["source"]["options"] = {"version": 999}
    manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    project.catalog.reload()
    assert project.run(spark=session).status == "failed"
    assert not (tmp_path / "data/result").exists()
    bindings["bindings"]["source"]["options"] = {"version": 0}
    manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    project.catalog.reload()
    assert project.run(spark=session).success
    assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 2


def test_spark_project_isolation_and_partial_failure_are_truthful(tmp_path, session):
    first = load_project(tmp_path / "first")
    second = load_project(tmp_path / "second")
    seed(first, session)
    session.range(10, 12).write.format("delta").save(str(second.root / "data/source"))
    plan = first.compile()
    with pytest.raises(PlanError):
        second.run(plan, spark=session)
    manifest = first.root / "definitions.yaml"
    definitions = yaml.safe_load(manifest.read_text())
    definitions["datasets"]["later"] = copy.deepcopy(definitions["datasets"]["result"])
    manifest.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    first.catalog.reload()
    with pytest.raises(PlanError):
        first.run(plan, spark=session)

    def crash(source):
        raise RuntimeError("private Spark credential or row")

    first._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node("publish", lambda source: source, {"source": "source"}, Output("result", WritePolicy())),
                    Node("crash", crash, {"source": "result"}, Output("later")),
                ],
                engine="spark",
            )
        }
    )
    result = first.run(spark=session)
    assert result.status == "failed"
    assert result.completed_nodes == ("publish",)
    assert result.publications[0].state == "committed"
    assert "private" not in json.dumps(result.to_dict())
    first.close()
    with patch("flint_core.core.base.EngineRegistry.get_engine", side_effect=AssertionError("legacy registry")):
        assert second.run(spark=session).success
    actual = session.read.format("delta").load(str(second.root / "data/result"))
    assert sorted(row.id for row in actual.collect()) == [10, 11]
    assert session.range(1).count() == 1


def test_spark_materializes_once_and_does_not_implicitly_persist_bound_outputs(tmp_path, session):
    from pyspark.sql import functions
    from pyspark.sql.types import LongType

    project = load_project(tmp_path)
    evaluated = session.sparkContext.accumulator(0)

    def evaluate(value):
        evaluated.add(1)
        return value

    calculate = functions.udf(evaluate, LongType()).asNondeterministic()
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node(
                        "produce",
                        lambda resource: resource.range(3).select(calculate("id").alias("id")),
                        {},
                        Output("result", WritePolicy()),
                        resources={"resource": "spark"},
                    )
                ],
                engine="spark",
            )
        }
    )
    assert project.run(spark=session).success
    assert evaluated.value == 3
    assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 3

    other = load_project(tmp_path / "other")
    other._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node(
                        "produce",
                        lambda resource: resource.range(1),
                        {},
                        Output("result"),
                        resources={"resource": "spark"},
                    )
                ],
                engine="spark",
            )
        }
    )
    assert other.run(spark=session).success
    assert list((other.root / "data").iterdir()) == []


def test_spark_concurrent_create_has_exactly_one_winner(tmp_path, session):
    project = load_project(tmp_path)

    def build(resource):
        return resource.range(2)

    declaration = Pipeline(
        "main",
        [Node("produce", build, {}, Output("result", WritePolicy()), resources={"resource": "spark"})],
        engine="spark",
    )
    other = FlintProject.load(tmp_path)
    project._pipelines = other._pipelines = MappingProxyType({"main": declaration})
    barrier = Barrier(2)
    real_rename = directory.rename_exclusive

    def race(stage, destination):
        barrier.wait(timeout=60)
        real_rename(stage, destination)

    with patch.object(directory, "rename_exclusive", race), ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda owner: owner.run(spark=session), [project, other]))
    assert sorted(result.status for result in results) == ["failed", "success"]
    assert sum(len(result.publications) for result in results) == 1
    assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 2
    assert not list((tmp_path / "data").glob(".flint-delta-*"))


def test_spark_post_commit_failure_returns_uncertain_and_preserves_table(tmp_path, session):
    project = load_project(tmp_path)
    seed(project, session)
    actual_sync = directory.sync_directory

    def broken_parent(path):
        if path == tmp_path / "data":
            raise OSError("private durability error")
        actual_sync(path)

    with patch.object(directory, "sync_directory", broken_parent):
        result = project.run(spark=session)
    assert result.status == "uncertain"
    assert result.publications[0].state == "uncertain"
    assert "private" not in json.dumps(result.to_dict())
    assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 2
    assert project.run(spark=session).status == "failed"


def test_spark_scaffold_compiles_without_session_and_runs_with_borrowed_session(tmp_path, session):
    root = tmp_path / "example"
    with patch("subprocess.run") as install:
        response = CliRunner().invoke(
            entry_point, ["init", "--format", "v1", "--engine", "spark", "--path", str(root), "--no-input"]
        )
    assert response.exit_code == 0, response.output
    install.assert_not_called()
    for command in (["validate"], ["run", "--dry-run"]):
        response = CliRunner().invoke(entry_point, [*command, "--project", str(root), "--json"])
        assert response.exit_code == 0, response.output
    with FlintProject.load(root) as project:
        assert project.compile().engine == "spark"
        assert project.run(spark=session).success
    rows = session.read.format("delta").load(str(root / "data/customers")).orderBy("id").collect()
    assert [row.name for row in rows] == ["Alice", "Bob"]


def test_spark_cli_owns_local_session_and_keeps_json_clean(tmp_path):
    from flint_core.runtime.scaffold import create_project

    create_project(tmp_path / "cli", engine="spark")
    command = [
        sys.executable,
        "-c",
        "from flint_core.cli import entry_point; entry_point()",
        "run",
        "--project",
        str(tmp_path / "cli"),
        "--spark-local",
        "--json",
    ]
    response = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert response.returncode == 0, response.stdout + response.stderr
    summary = json.loads(response.stdout)
    assert summary["status"] == "success"
    assert summary["publications"][0]["state"] == "committed"
    repeated = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert repeated.returncode == 2
    assert json.loads(repeated.stdout)["publications"] == []


def test_delta_replaced_stage_and_symlink_sources_are_preserved(tmp_path):
    project = load_project(tmp_path, "pandas")
    publisher = directory.DirectoryPublisher()

    def replace_stage(stage):
        moved = stage.with_name("owned-stage-backup")
        stage.rename(moved)
        stage.mkdir()
        (stage / "foreign").write_bytes(b"preserve")

    with pytest.raises(DataValidationError) as error:
        publisher.publish(project.catalog.snapshot, "result", replace_stage)
    assert error.value.cleanup_pending
    assert not (tmp_path / "data/result").exists()
    assert next((tmp_path / "data").glob(".flint-delta-*/foreign")).read_bytes() == b"preserve"
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "data/source").symlink_to(outside, target_is_directory=True)
    assert project.run().error_code == "data_validation_failed"
    assert list(outside.iterdir()) == []


def test_spark_bad_external_input_never_reaches_transformation(tmp_path, session):
    from unittest.mock import Mock

    project = load_project(tmp_path)
    session.createDataFrame([("wrong",)], "id string").write.format("delta").save(str(tmp_path / "data/source"))
    compute = Mock(side_effect=AssertionError("must not execute"))

    def transform(source):
        return compute(source)

    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [Node("validate_first", transform, {"source": "source"}, Output("result", WritePolicy()))],
                engine="spark",
            )
        }
    )
    result = project.run(spark=session)
    assert result.error_code == "data_validation_failed"
    compute.assert_not_called()
    assert not (tmp_path / "data/result").exists()


def test_spark_cleanup_failure_is_reported_without_stopping_borrowed_session(tmp_path, session):
    from flint_core.runtime.spark import SparkFrames

    project = load_project(tmp_path)
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node(
                        "produce",
                        lambda resource: resource.range(1),
                        {},
                        Output("result"),
                        resources={"resource": "spark"},
                    )
                ],
                engine="spark",
            )
        }
    )
    captured = []
    release = SparkFrames.release

    def broken(owner, frame):
        captured.append(owner)
        raise OSError("cannot confirm checkpoint cleanup")

    try:
        with patch.object(SparkFrames, "release", broken):
            result = project.run(spark=session)
        assert result.success and result.cleanup_pending
        assert session.range(1).count() == 1
    finally:
        for owner in captured:
            for retained, _, _ in tuple(owner._owned.values()):
                release(owner, retained)


def test_spark_cli_uncertain_exit_matches_python(tmp_path, session):
    project = load_project(tmp_path)
    seed(project, session)
    actual_sync = directory.sync_directory

    def fail_parent(path):
        if path == tmp_path / "data":
            raise OSError("unknown parent durability")
        actual_sync(path)

    with (
        patch("flint_core.runtime.spark.open_session", return_value=(session, False)),
        patch.object(directory, "sync_directory", fail_parent),
    ):
        response = CliRunner().invoke(entry_point, ["run", "--project", str(tmp_path), "--spark-local", "--json"])
    assert response.exit_code == 3
    result = json.loads(response.output)
    assert result["status"] == "uncertain"
    assert result["publications"][0]["state"] == "uncertain"
    assert session.read.format("delta").load(str(tmp_path / "data/result")).count() == 2


def test_historical_writes_and_overlapping_destinations_are_rejected(tmp_path):
    root = project_files(tmp_path)
    manifest = root / "bindings.yaml"
    bindings = yaml.safe_load(manifest.read_text())
    bindings["bindings"]["result"]["options"] = {"version": 0}
    manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    with pytest.raises(ProjectError):
        FlintProject.load(root)
    bindings["bindings"]["result"]["options"] = {}
    bindings["bindings"]["result"]["target"]["path"] = "data/source/child"
    manifest.write_text(yaml.safe_dump(bindings), encoding="utf-8")
    with pytest.raises(PlanError):
        FlintProject.load(root).compile()


def test_delta_external_file_references_and_symlinked_logs_fail_closed(tmp_path):
    from flint_core.runtime.local_delta import _check_files

    project = load_project(tmp_path, "pandas")
    root = tmp_path / "data/source"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"unrelated")
    for location in (str(outside), "s3://bucket/file.parquet", "../outside", "file://remote-host/file"):
        with pytest.raises(DataValidationError):
            _check_files(root, [location])
    (root / "_delta_log").symlink_to(tmp_path, target_is_directory=True)
    assert project.run().error_code == "data_validation_failed"
    assert outside.read_bytes() == b"unrelated"


def test_delta_staging_rejects_absolute_data_references(tmp_path):
    from flint_core.runtime.local_delta import _check_prepared

    stage = tmp_path / "stage"
    (stage / "_delta_log").mkdir(parents=True)
    (stage / "file.parquet").write_bytes(b"placeholder")
    (stage / "_delta_log/00000000000000000000.json").write_text(
        json.dumps({"add": {"path": str(stage / "file.parquet")}}) + "\n", encoding="utf-8"
    )
    with pytest.raises(DataValidationError):
        _check_prepared(stage)


@pytest.mark.parametrize("engine", ["spark", "pandas"])
def test_delta_read_handles_spaces_in_paths(tmp_path, request, engine):
    project = load_project(tmp_path / "space project", engine)
    spark = request.getfixturevalue("session") if engine == "spark" else None
    seed(project, spark)
    result = project.run(spark=spark)
    assert result.success, result.to_dict()
    if spark is None:
        assert PandasDeltaAdapter().read(project.catalog.snapshot, "result")["id"].tolist() == [0, 1]
    else:
        assert spark.read.format("delta").load(str(project.root / "data/result")).count() == 2


def test_local_spark_ownership_rechecks_context_under_sdk_locks():
    context_type = pytest.importorskip("pyspark").SparkContext
    session_type = pytest.importorskip("pyspark.sql").SparkSession
    pytest.importorskip("delta")
    from flint_core.runtime.spark import open_session

    def concurrent_creation(builder):
        context_type._active_spark_context = object()
        return builder

    with (
        patch.object(context_type, "_active_spark_context", None),
        patch("delta.configure_spark_with_delta_pip", concurrent_creation),
        patch.object(session_type.Builder, "getOrCreate") as start,
    ):
        with pytest.raises(DataValidationError):
            open_session(local=True)
    start.assert_not_called()


def test_local_spark_cannot_implicitly_use_connect_environment(monkeypatch):
    context_type = pytest.importorskip("pyspark").SparkContext
    from flint_core.runtime.spark import open_session

    monkeypatch.setenv("SPARK_REMOTE", "sc://not-a-local-session")
    with patch.object(context_type, "_active_spark_context", None):
        with pytest.raises(DataValidationError):
            open_session(local=True)


@pytest.mark.parametrize("engine", ["spark", "pandas"])
@pytest.mark.parametrize("character", ["%20", "?", "#"])
def test_ambiguous_delta_paths_fail_metadata_before_native_io(tmp_path, engine, character):
    root = project_files(tmp_path / ("ambiguous" + character), engine)
    with pytest.raises(ProjectError):
        FlintProject.load(root)
    assert list((root / "data").iterdir()) == []


def test_directory_create_never_replaces_even_an_empty_competing_directory(tmp_path):
    project = load_project(tmp_path, "pandas")
    real_rename = directory.rename_exclusive

    def create_empty(stage, destination):
        destination.mkdir()
        real_rename(stage, destination)

    def prepare(stage):
        (stage / "owned").write_bytes(b"owned")

    with patch.object(directory, "rename_exclusive", create_empty):
        with pytest.raises(PublicationConflict):
            directory.DirectoryPublisher().publish(project.catalog.snapshot, "result", prepare)
    assert list((tmp_path / "data/result").iterdir()) == []
    assert not list((tmp_path / "data").glob(".flint-delta-*"))


def test_pandas_delta_runtime_validation_is_independent_of_csv_adapter(tmp_path):
    from flint_core.runtime.local_csv import LocalCsvAdapter

    project = load_project(tmp_path, engine="pandas")
    seed(project)
    with patch.object(LocalCsvAdapter, "validate", side_effect=AssertionError("CSV must not validate Delta frames")):
        result = project.run()
    assert result.success, result.to_dict()
    assert result.publications[0].state == "committed"
    assert PandasDeltaAdapter().read(project.catalog.snapshot, "result")["id"].tolist() == [0, 1]
