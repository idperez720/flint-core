"""Regression coverage for catalog identity and snapshot safety."""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog import CatalogParseError, DataCatalog
from flint_core.core.initializer import LAYOUT_SUBDIRS, DirectoryStructureStep, SampleCatalogStep, ScaffoldContext


def _manifest(dataset_name: str = "orders", storage_path: str = "orders.csv") -> str:
    return yaml.safe_dump(
        {dataset_name: {"engine": "pandas", "format": "csv", "storage_path": storage_path}}, sort_keys=False
    )


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "content, key, first_line, second_line",
    [
        (_manifest() + _manifest(), "orders", 1, 5),
        ("orders:\n  engine: pandas\n  engine: spark\n", "engine", 2, 3),
        ("orders:\n  options:\n    header: true\n    header: false\n", "header", 3, 4),
        ("orders:\n  columns:\n    - name: id\n      name: other\n", "name", 3, 4),
        (".base:\n  format: csv\n  format: parquet\n", "format", 2, 3),
        (".base: &base\n  engine: pandas\norders:\n  <<: *base\n  engine: spark\n", "engine", 2, 5),
        (".base: &base {}\norders:\n  <<: *base\n  <<: *base\n", "<<", 3, 4),
    ],
)
def test_duplicate_yaml_keys_report_both_locations(
    tmp_path: Path, content: str, key: str, first_line: int, second_line: int
) -> None:
    manifest = _write(tmp_path / "catalog.yaml", content)
    with pytest.raises(CatalogParseError) as captured:
        DataCatalog(manifest)
    message = str(captured.value)
    assert key in message
    assert str(manifest) in message
    assert f"line {first_line}" in message
    assert f"line {second_line}" in message
    assert "column" in message


def test_duplicate_ids_across_same_named_manifests_report_full_paths(tmp_path: Path) -> None:
    first = _write(tmp_path / "a" / "datasets.yaml", _manifest())
    second = _write(tmp_path / "b" / "datasets.yml", _manifest(storage_path="other.csv"))
    with pytest.raises(CatalogParseError) as captured:
        DataCatalog(tmp_path)
    message = str(captured.value)
    assert "orders" in message
    assert f"{first}:1:1" in message
    assert f"{second}:1:1" in message


@pytest.mark.parametrize(
    "failure",
    ["syntax", "duplicate_key", "duplicate_id", "mandatory", "model", "root", "body", "missing", "extension"],
)
def test_failed_reload_keeps_previous_snapshot(tmp_path: Path, failure: str) -> None:
    original = _write(tmp_path / "original.yaml", _manifest("preserved"))
    catalog = DataCatalog(original)
    previous = catalog["preserved"]
    candidate = tmp_path / "candidate"
    _write(candidate / "a.yaml", _manifest("replacement"))
    contents = {
        "syntax": "broken: [",
        "duplicate_key": _manifest("replacement") + _manifest("replacement"),
        "duplicate_id": _manifest("replacement"),
        "mandatory": "broken:\n  format: csv\n",
        "model": "broken:\n  engine: [pandas]\n  format: csv\n  storage_path: bad.csv\n",
        "root": "- not-a-catalog\n",
        "body": "broken: not-a-definition\n",
    }
    if failure in contents:
        _write(candidate / "z.yaml", contents[failure])
    elif failure == "missing":
        candidate = tmp_path / "missing"
    else:
        candidate = _write(tmp_path / "unsupported.txt", _manifest("replacement"))
    expected_error = (
        FileNotFoundError if failure == "missing" else KeyError if failure == "mandatory" else CatalogParseError
    )
    with pytest.raises(expected_error):
        catalog.reload_catalog(candidate)
    assert catalog.dataset_names == ["preserved"]
    assert catalog["preserved"] is previous
    assert catalog.preserved is previous
    assert "replacement" not in catalog
    assert not hasattr(catalog, "replacement")


def test_candidate_is_not_visible_during_compilation(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "original.yaml", _manifest("preserved")))
    previous = catalog["preserved"]
    candidate = tmp_path / "candidate"
    _write(candidate / "a.yaml", _manifest("replacement"))
    _write(candidate / "z.yaml", _manifest("last"))
    parse_file = catalog._parse_file
    visited = []

    def inspect_candidate(*args, **kwargs):
        result = parse_file(*args, **kwargs)
        visited.append(args[0].name)
        assert catalog.dataset_names == ["preserved"]
        assert catalog["preserved"] is previous
        return result

    with patch.object(catalog, "_parse_file", side_effect=inspect_candidate):
        catalog.reload_catalog(candidate)
    assert visited == ["a.yaml", "z.yaml"]
    assert catalog.dataset_names == ["replacement", "last"]


def test_successful_reload_replaces_snapshot_and_removes_dot_access(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "first.yaml", _manifest("old_snapshot_id")))
    catalog.reload_catalog(_write(tmp_path / "second.yaml", _manifest("new_snapshot_id")))
    assert catalog.dataset_names == ["new_snapshot_id"]
    assert catalog.new_snapshot_id is catalog["new_snapshot_id"]
    assert not hasattr(catalog, "old_snapshot_id")
    with pytest.raises(KeyError):
        catalog["old_snapshot_id"]
    with pytest.raises(AttributeError):
        catalog.unknown_snapshot_id


def test_catalog_attributes_are_instance_scoped(tmp_path: Path) -> None:
    first = DataCatalog(_write(tmp_path / "first.yaml", _manifest("instance_only_id")))
    second = DataCatalog(_write(tmp_path / "second.yaml", _manifest("separate_id")))
    assert first.instance_only_id is first["instance_only_id"]
    assert second.separate_id is second["separate_id"]
    assert not hasattr(DataCatalog, "instance_only_id")
    assert not hasattr(second, "instance_only_id")
    assert not hasattr(first, "separate_id")


def test_same_dataset_id_has_separate_bindings_per_instance(tmp_path: Path) -> None:
    first = DataCatalog(_write(tmp_path / "first.yaml", _manifest(storage_path="first.csv")))
    second = DataCatalog(_write(tmp_path / "second.yaml", _manifest(storage_path="second.csv")))
    assert first.orders.storage_path == "first.csv"
    assert second.orders.storage_path == "second.csv"
    assert first.orders is not second.orders


def test_dataset_ids_do_not_shadow_catalog_api(tmp_path: Path) -> None:
    manifest = _write(tmp_path / "catalog.yaml", _manifest("load") + _manifest("dataset_names"))
    catalog = DataCatalog(manifest)
    assert callable(catalog.load)
    assert catalog["load"].name == "load"
    assert catalog["dataset_names"].name == "dataset_names"
    assert catalog.dataset_names == ["load", "dataset_names"]


@pytest.mark.asyncio
async def test_failed_async_reload_preserves_snapshot(tmp_path: Path) -> None:
    original = _write(tmp_path / "original.yaml", _manifest("async_preserved"))
    catalog = DataCatalog(original)
    previous = catalog["async_preserved"]
    broken = _write(tmp_path / "broken.yaml", _manifest() + _manifest())
    with pytest.raises(CatalogParseError):
        await catalog.reload_catalog_async(broken)
    assert catalog["async_preserved"] is previous
    assert catalog.dataset_names == ["async_preserved"]


def test_read_failure_does_not_publish_partial_candidate(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "original.yaml", _manifest("preserved")))
    previous = catalog["preserved"]
    candidate = tmp_path / "candidate"
    _write(candidate / "a.yaml", _manifest("replacement"))
    _write(candidate / "z.yaml", _manifest("last"))
    open_file = Path.open

    def fail_read(file_path, *args, **kwargs):
        if file_path.name == "z.yaml":
            raise OSError("Read interrupted")
        return open_file(file_path, *args, **kwargs)

    with patch.object(Path, "open", new=fail_read):
        with pytest.raises(OSError, match="Read interrupted"):
            catalog.reload_catalog(candidate)
    assert catalog.dataset_names == ["preserved"]
    assert catalog["preserved"] is previous


def test_directory_discovery_failure_does_not_silently_drop_manifests(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "original.yaml", _manifest("preserved")))
    previous = catalog["preserved"]
    candidate = tmp_path / "candidate"
    _write(candidate / "a.yaml", _manifest("replacement"))
    blocked = _write(candidate / "nested" / "orders.yaml", _manifest()).parent
    scan_directory = os.scandir

    def fail_discovery(directory):
        if Path(directory) == blocked:
            raise PermissionError("Cannot list catalog directory")
        return scan_directory(directory)

    with patch("os.scandir", side_effect=fail_discovery):
        with pytest.raises(PermissionError, match="Cannot list"):
            catalog.reload_catalog(candidate)
    assert catalog.dataset_names == ["preserved"]
    assert catalog["preserved"] is previous


def test_concurrent_reader_never_observes_a_failed_candidate(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "original.yaml", _manifest("preserved")))
    previous = catalog["preserved"]
    candidate = _write(tmp_path / "candidate.yaml", _manifest("replacement"))
    prepared = Event()
    release = Event()
    reading = Event()
    parse_file = catalog._parse_file

    def fail_after_preparation(*args, **kwargs):
        parse_file(*args, **kwargs)
        prepared.set()
        assert release.wait(timeout=5)
        raise CatalogParseError("Candidate rejected")

    def read_snapshot():
        reading.set()
        return catalog.dataset_names, catalog["preserved"]

    with patch.object(catalog, "_parse_file", side_effect=fail_after_preparation):
        with ThreadPoolExecutor(max_workers=2) as executor:
            reload_result = executor.submit(catalog.reload_catalog, candidate)
            try:
                assert prepared.wait(timeout=5)
                reader = executor.submit(read_snapshot)
                assert reading.wait(timeout=5)
            finally:
                release.set()
            with pytest.raises(CatalogParseError, match="Candidate rejected"):
                reload_result.result(timeout=5)
            assert reader.result(timeout=5) == (["preserved"], previous)


def test_yaml_defaults_without_collisions_remain_supported(tmp_path: Path) -> None:
    manifest = _write(
        tmp_path / "catalog.yaml",
        ".base: &base\n  engine: pandas\n  format: csv\n"
        "orders:\n  <<: *base\n  storage_path: orders.csv\n"
        "customers:\n  <<: *base\n  storage_path: customers.csv\n",
    )
    catalog = DataCatalog(manifest)
    assert catalog.dataset_names == ["orders", "customers"]
    assert catalog["orders"].engine == "pandas"
    assert catalog["customers"].storage_path == "customers.csv"


def test_colliding_multiple_merge_sources_are_rejected(tmp_path: Path) -> None:
    manifest = _write(
        tmp_path / "catalog.yaml",
        ".first: &first\n  engine: pandas\n.second: &second\n  engine: spark\n"
        "orders:\n  <<: [*first, *second]\n  format: csv\n  storage_path: orders.csv\n",
    )
    with pytest.raises(CatalogParseError, match="Duplicate YAML key 'engine'"):
        DataCatalog(manifest)


def test_duplicate_diagnostics_do_not_include_values(tmp_path: Path) -> None:
    manifest = _write(tmp_path / "catalog.yaml", "orders:\n  password: first-secret\n  password: second-secret\n")
    with pytest.raises(CatalogParseError) as captured:
        DataCatalog(manifest)
    assert "password" in str(captured.value)
    assert "first-secret" not in str(captured.value)
    assert "second-secret" not in str(captured.value)


def test_unsafe_yaml_tags_cannot_execute_code(tmp_path: Path) -> None:
    manifest = _write(tmp_path / "catalog.yaml", '!!python/object/apply:os.system ["untrusted-command"]')
    with patch("os.system") as command:
        with pytest.raises(CatalogParseError):
            DataCatalog(manifest)
    command.assert_not_called()


@pytest.mark.parametrize("key", ["123", "true", "null", "''"])
def test_dataset_ids_must_be_nonempty_strings(tmp_path: Path, key: str) -> None:
    manifest = _write(
        tmp_path / "catalog.yaml", f"{key}:\n  engine: pandas\n  format: csv\n  storage_path: orders.csv\n"
    )
    with pytest.raises(CatalogParseError):
        DataCatalog(manifest)


def test_empty_catalog_is_an_explicit_valid_snapshot(tmp_path: Path) -> None:
    catalog = DataCatalog(_write(tmp_path / "original.yaml", _manifest()))
    empty = tmp_path / "empty"
    empty.mkdir()
    catalog.reload_catalog(empty)
    assert catalog.dataset_names == []
    assert not hasattr(catalog, "orders")


def test_missing_discovered_catalog_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path / "pyproject.toml", '[project]\nname = "test"\n')
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        DataCatalog()


@pytest.mark.parametrize("pattern", [*LAYOUT_SUBDIRS, "datamart", "datamesh"])
def test_every_generated_layout_has_unique_explicit_ids(tmp_path: Path, pattern: str) -> None:
    context = ScaffoldContext(
        root_path=tmp_path,
        name="test",
        version="0.1.0",
        description="Test",
        author="Test",
        metadata={"pattern": pattern, "domains": ["finance", "sales"], "envs": ["dev"]},
    )
    DirectoryStructureStep().execute(context)
    SampleCatalogStep().execute(context)
    manifests = sorted((tmp_path / "conf" / "catalog").rglob("*.yaml"))
    declared_ids = [name for manifest in manifests for name in yaml.safe_load(manifest.read_text())]
    expected = (
        ["finance.domain_dataset", "sales.domain_dataset"]
        if pattern in ("datamart", "datamesh")
        else [f"{layer}.sample_table" for layer in LAYOUT_SUBDIRS[pattern]]
        if pattern != "default"
        else ["sample_table"]
    )
    assert sorted(declared_ids) == sorted(expected)
    assert len(declared_ids) == len(set(declared_ids))
    catalog = DataCatalog(tmp_path / "conf" / "catalog")
    assert sorted(catalog.dataset_names) == sorted(expected)


def test_generated_domain_ids_are_yaml_safe(tmp_path: Path) -> None:
    context = ScaffoldContext(
        root_path=tmp_path,
        name="test",
        version="0.1.0",
        description="Test",
        author="Test",
        metadata={"pattern": "datamesh", "domains": ["sales: west", "team's data"], "envs": ["dev"]},
    )
    DirectoryStructureStep().execute(context)
    SampleCatalogStep().execute(context)
    catalog = DataCatalog(tmp_path / "conf" / "catalog")
    assert sorted(catalog.dataset_names) == ["sales: west.domain_dataset", "team's data.domain_dataset"]
