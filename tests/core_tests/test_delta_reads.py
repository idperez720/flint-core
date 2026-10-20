"""Real local Delta reads against the supported deltalake dependency."""

import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from flint_core.core.catalog import DataCatalog

pandas = pytest.importorskip("pandas")
deltalake = pytest.importorskip("deltalake")
DeltaFormatHandler = pytest.importorskip("flint_core.pandas_core.engine").DeltaFormatHandler


@pytest.fixture
def delta_snapshots(tmp_path: Path):
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "delta-reads"\n', encoding="utf-8")
    manifest = tmp_path / "catalog.yaml"
    manifest.write_text("events:\n  engine: pandas\n  format: delta\n  storage_path: data/events\n", encoding="utf-8")
    catalog = DataCatalog(manifest)
    catalog.save(pandas.DataFrame({"id": [1], "value": ["first"]}), "events")
    target = tmp_path / "data" / "events"
    first_timestamp = deltalake.DeltaTable(target).history()[0]["timestamp"]
    deadline = time.monotonic() + 5
    while int(time.time() * 1000) <= first_timestamp:
        if time.monotonic() >= deadline:
            pytest.fail("Clock did not advance past the first Delta commit")
        time.sleep(0.001)
    catalog.save(pandas.DataFrame({"id": [2], "value": ["second"]}), "events", mode="append")
    timestamp = datetime.fromtimestamp(first_timestamp / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")
    return catalog, target, timestamp


def _table_contents(target: Path):
    return {str(path.relative_to(target)): path.read_bytes() for path in target.rglob("*") if path.is_file()}


def test_delta_latest_read_round_trip(delta_snapshots) -> None:
    catalog, _, _ = delta_snapshots
    actual = catalog.load("events").sort_values("id")
    assert actual.to_dict("list") == {"id": [1, 2], "value": ["first", "second"]}


@pytest.mark.parametrize("version", [0, "0"])
def test_delta_version_zero_is_not_latest(delta_snapshots, version) -> None:
    catalog, _, _ = delta_snapshots
    actual = catalog.load("events", version=version)
    assert actual.to_dict("list") == {"id": [1], "value": ["first"]}


@pytest.mark.parametrize("via_options", [False, True])
def test_delta_timestamp_selects_historical_snapshot(delta_snapshots, via_options: bool) -> None:
    catalog, _, timestamp = delta_snapshots
    arguments = {"options": {"timestampAsOf": timestamp}} if via_options else {"as_of": timestamp}
    actual = catalog.load("events", **arguments)
    assert actual.to_dict("list") == {"id": [1], "value": ["first"]}


def test_delta_native_options_and_input_mapping_are_preserved() -> None:
    options = {"versionAsOf": "0", "storage_options": {"endpoint": "test"}, "without_files": False}
    with patch("deltalake.DeltaTable") as table:
        DeltaFormatHandler().read("data/events", {}, [], options)
    table.assert_called_once_with("data/events", version=0, storage_options={"endpoint": "test"}, without_files=False)
    assert options == {"versionAsOf": "0", "storage_options": {"endpoint": "test"}, "without_files": False}
    table.return_value.load_as_version.assert_not_called()


@pytest.mark.parametrize("version", [-1, True, 1.5, "", "latest", " 0"])
def test_invalid_delta_versions_fail_before_access(version) -> None:
    with patch("deltalake.DeltaTable") as table:
        with pytest.raises(ValueError, match="versionAsOf"):
            DeltaFormatHandler().read("data/events", {}, [], {"versionAsOf": version})
    table.assert_not_called()


@pytest.mark.parametrize("timestamp", ["", " ", 123, False])
def test_invalid_timestamp_types_fail_before_access(timestamp) -> None:
    with patch("deltalake.DeltaTable") as table:
        with pytest.raises(ValueError, match="timestampAsOf"):
            DeltaFormatHandler().read("data/events", {}, [], {"timestampAsOf": timestamp})
    table.assert_not_called()


@pytest.mark.parametrize("via_options", [False, True])
def test_conflicting_snapshot_selectors_fail_before_access(delta_snapshots, via_options: bool) -> None:
    catalog, target, timestamp = delta_snapshots
    previous = _table_contents(target)
    arguments = (
        {"version": 0, "options": {"timestampAsOf": timestamp}} if via_options else {"version": 0, "as_of": timestamp}
    )
    with patch("deltalake.DeltaTable") as table:
        with pytest.raises(ValueError, match="versionAsOf.*timestampAsOf"):
            catalog.load("events", **arguments)
    table.assert_not_called()
    assert _table_contents(target) == previous


@pytest.mark.parametrize("arguments", [{"version": 999}, {"as_of": "not-a-timestamp"}])
def test_native_snapshot_errors_propagate_without_latest_fallback(delta_snapshots, arguments) -> None:
    catalog, target, _ = delta_snapshots
    previous = _table_contents(target)
    with pytest.raises((deltalake.exceptions.DeltaError, ValueError)):
        catalog.load("events", **arguments)
    assert _table_contents(target) == previous


def test_missing_delta_table_is_not_created_by_read(tmp_path: Path) -> None:
    target = tmp_path / "absent"
    with pytest.raises(deltalake.exceptions.DeltaError):
        DeltaFormatHandler().read(str(target), {}, [], {})
    assert not target.exists()
