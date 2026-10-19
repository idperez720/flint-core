"""Real CSV contract checking and atomic create evidence."""

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from flint_core import FlintProject
from flint_core.runtime.errors import DataValidationError, PublicationUncertain
from flint_core.runtime.local_csv import LocalCsvAdapter

from .test_project import make_project


def target(root):
    project = FlintProject.load(make_project(root))
    (root / "data").mkdir(exist_ok=True)
    return project.catalog.snapshot


def test_create_round_trip_and_second_create_preserves_bytes(tmp_path):
    snapshot = target(tmp_path)
    adapter = LocalCsvAdapter()
    adapter.publish(snapshot, "result", pd.DataFrame({"id": [1, 2]}))
    path = snapshot.bindings["result"].target.path
    previous = path.read_bytes()
    assert adapter.read(snapshot, "result")["id"].tolist() == [1, 2]
    with pytest.raises(FileExistsError):
        adapter.publish(snapshot, "result", pd.DataFrame({"id": [3]}))
    assert path.read_bytes() == previous
    assert list(path.parent.glob(".flint-*")) == []


@pytest.mark.parametrize(
    "frame",
    [pd.DataFrame({"wrong": [1]}), pd.DataFrame({"id": ["1"]}), pd.DataFrame({"id": pd.array([None], dtype="Int64")})],
)
def test_invalid_data_never_publishes(tmp_path, frame):
    snapshot = target(tmp_path)
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().publish(snapshot, "result", frame)
    assert not snapshot.bindings["result"].target.path.exists()
    assert not list((tmp_path / "data").iterdir())


def test_concurrent_creation_is_not_overwritten(tmp_path):
    snapshot = target(tmp_path)
    path = snapshot.bindings["result"].target.path
    link = os.link

    def racing_create(stage, destination):
        path.write_bytes(b"competing-writer\n")
        return link(stage, destination)

    with patch("os.link", side_effect=racing_create):
        with pytest.raises(FileExistsError):
            LocalCsvAdapter().publish(snapshot, "result", pd.DataFrame({"id": [1]}))
    assert path.read_bytes() == b"competing-writer\n"
    assert not list(path.parent.glob(".flint-*"))


def test_preparation_failure_leaves_target_absent_and_cleans_owned_stage(tmp_path):
    snapshot = target(tmp_path)
    with patch("os.fsync", side_effect=OSError("interrupted preparation")):
        with pytest.raises(DataValidationError):
            LocalCsvAdapter().publish(snapshot, "result", pd.DataFrame({"id": [1]}))
    assert not list((tmp_path / "data").iterdir())


def test_post_commit_failure_is_uncertain_and_never_deletes_target(tmp_path):
    snapshot = target(tmp_path)
    adapter = LocalCsvAdapter()
    with patch.object(adapter, "_sync_parent", side_effect=OSError("uncertain durability")):
        with pytest.raises(PublicationUncertain):
            adapter.publish(snapshot, "result", pd.DataFrame({"id": [1]}))
    assert adapter.read(snapshot, "result")["id"].tolist() == [1]


@pytest.mark.parametrize("contents", ["id,id\n1,2\n", "id\n1,2\n", "id\n9223372036854775808\n", "id\n1.5\n"])
def test_bad_csv_is_not_silently_repaired(tmp_path, contents):
    snapshot = target(tmp_path)
    snapshot.bindings["source"].target.path.write_text(contents, encoding="utf-8")
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().read(snapshot, "source")


def test_symlink_target_is_rejected(tmp_path):
    snapshot = target(tmp_path)
    external = tmp_path / "external.csv"
    external.write_bytes(b"id\n1\n")
    snapshot.bindings["source"].target.path.symlink_to(external)
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().read(snapshot, "source")
    assert external.read_bytes() == b"id\n1\n"


def typed_snapshot(root, *, extra_columns="forbid"):
    make_project(root)
    path = root / "definitions.yaml"
    data = yaml.safe_load(path.read_text())
    for definition in data["datasets"].values():
        definition["schema"] = {
            "extra_columns": extra_columns,
            "columns": {
                "id": {"type": "int64", "nullable": False},
                "text": {"type": "string", "nullable": True},
                "flag": {"type": "boolean", "nullable": True},
                "score": {"type": "float64", "nullable": True},
            },
        }
        definition["constraints"] = [{"id": "unique_id", "type": "unique", "columns": ["id"], "severity": "error"}]
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    (root / "data").mkdir(exist_ok=True)
    return FlintProject.load(root).catalog.snapshot


def typed_frame():
    return pd.DataFrame(
        {
            "id": pd.array([1, 2, 3, 4], dtype="Int64"),
            "text": pd.array(["", "\\N", "\\prefix", None], dtype="string"),
            "flag": pd.array([True, False, None, True], dtype="boolean"),
            "score": pd.array([1.25, -2.5, None, 0.0], dtype="Float64"),
        }
    )


def test_all_four_types_and_null_codec_round_trip_without_conversion(tmp_path):
    snapshot = typed_snapshot(tmp_path)
    frame = typed_frame()
    adapter = LocalCsvAdapter()
    adapter.publish(snapshot, "result", frame)
    pd.testing.assert_frame_equal(adapter.read(snapshot, "result"), frame)


def test_uniqueness_and_extra_column_policy_are_executed(tmp_path):
    snapshot = typed_snapshot(tmp_path)
    frame = typed_frame()
    frame.loc[1, "id"] = 1
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().publish(snapshot, "result", frame)
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().publish(snapshot, "result", typed_frame().assign(extra="unexpected"))
    assert not list((tmp_path / "data").iterdir())


def test_allow_preserves_opaque_extra_strings(tmp_path):
    snapshot = typed_snapshot(tmp_path, extra_columns="allow")
    frame = typed_frame().assign(extra=pd.array(["001", "NA", "", None], dtype="string"))
    adapter = LocalCsvAdapter()
    adapter.publish(snapshot, "result", frame)
    pd.testing.assert_frame_equal(adapter.read(snapshot, "result"), frame)


def test_non_finite_numbers_and_boolean_text_are_not_coerced(tmp_path):
    snapshot = typed_snapshot(tmp_path)
    frame = typed_frame()
    frame.loc[0, "score"] = float("inf")
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().publish(snapshot, "result", frame)
    source = snapshot.bindings["source"].target.path
    source.write_text("id,text,flag,score\n1,name,1,1.5\n", encoding="utf-8")
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().read(snapshot, "source")


def test_cleanup_retains_replaced_staging_file(tmp_path):
    snapshot = target(tmp_path)
    adapter = LocalCsvAdapter()
    sync_parent = adapter._sync_parent

    def replace_stage(parent):
        stage = next(parent.glob(".flint-*.tmp"))
        stage.rename(parent / "owned-backup")
        stage.write_bytes(b"foreign-content")
        sync_parent(parent)

    with patch.object(adapter, "_sync_parent", side_effect=replace_stage):
        receipt = adapter.publish(snapshot, "result", pd.DataFrame({"id": [1]}))
    assert receipt.cleanup_pending
    assert next((tmp_path / "data").glob(".flint-*.tmp")).read_bytes() == b"foreign-content"
    assert adapter.read(snapshot, "result")["id"].tolist() == [1]


def test_empty_typed_batch_is_still_a_valid_csv(tmp_path):
    snapshot = typed_snapshot(tmp_path)
    frame = typed_frame().iloc[:0]
    adapter = LocalCsvAdapter()
    adapter.publish(snapshot, "result", frame)
    pd.testing.assert_frame_equal(adapter.read(snapshot, "result"), frame.reset_index(drop=True))


def test_all_null_float_column_is_not_treated_as_a_string(tmp_path):
    snapshot = typed_snapshot(tmp_path)
    frame = typed_frame()
    frame["text"] = pd.Series([float("nan")] * 4, dtype="float64")
    with pytest.raises(DataValidationError):
        LocalCsvAdapter().publish(snapshot, "result", frame)


def test_two_actual_writers_have_exactly_one_create_winner(tmp_path):
    snapshot = target(tmp_path)
    barrier = Barrier(2)
    link = os.link

    def synchronize_link(stage, destination):
        barrier.wait(timeout=5)
        return link(stage, destination)

    def writer(number):
        try:
            LocalCsvAdapter().publish(snapshot, "result", pd.DataFrame({"id": [number]}))
            return "committed"
        except FileExistsError:
            return "conflict"

    with patch("os.link", side_effect=synchronize_link), ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(writer, 1)
        second = executor.submit(writer, 2)
        assert sorted([first.result(timeout=10), second.result(timeout=10)]) == ["committed", "conflict"]
    assert LocalCsvAdapter().read(snapshot, "result")["id"].tolist() in ([1], [2])
    assert not list((tmp_path / "data").glob(".flint-*"))


def test_link_error_after_effect_is_uncertain_not_retried(tmp_path):
    snapshot = target(tmp_path)
    link = os.link

    def interrupted_link(stage, destination):
        link(stage, destination)
        raise OSError("uncertain system call result")

    with patch("os.link", side_effect=interrupted_link) as publication:
        with pytest.raises(PublicationUncertain):
            LocalCsvAdapter().publish(snapshot, "result", pd.DataFrame({"id": [1]}))
    assert publication.call_count == 1
    assert LocalCsvAdapter().read(snapshot, "result")["id"].tolist() == [1]
