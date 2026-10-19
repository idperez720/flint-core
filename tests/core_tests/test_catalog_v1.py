"""Strict, metadata-only v1 catalog contract regressions."""

import copy
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog.v1 import ManifestCatalog
from flint_core.core.exceptions import CatalogParseError


def _definitions():
    return {
        "schema_version": 1,
        "kind": "DatasetDefinitions",
        "datasets": {
            "raw.customers": {
                "schema": {
                    "extra_columns": "forbid",
                    "columns": {
                        "id": {"type": "int64", "nullable": False},
                        "name": {"type": "string", "nullable": True},
                        "active": {"type": "boolean", "nullable": False},
                        "score": {"type": "float64", "nullable": True},
                    },
                },
                "constraints": [{"id": "unique_id", "type": "unique", "columns": ["id"], "severity": "error"}],
                "governance": {"owner": "engineering", "tags": ["customers"]},
                "metadata": {"nested": {"values": [1, True, None, "note"]}},
            },
            "virtual.customers": {
                "schema": {"extra_columns": "allow", "columns": {"id": {"type": "int64", "nullable": False}}}
            },
        },
    }


def _bindings():
    return {
        "schema_version": 1,
        "kind": "DatasetBindings",
        "environment": "dev",
        "bindings": {
            "raw.customers": {
                "adapter": "pandas.csv",
                "format": "csv",
                "target": {"kind": "file", "path": "data/customers.csv"},
                "access": "read_only",
                "options": {},
            }
        },
    }


@pytest.fixture(autouse=True, params=["spark.delta", "pandas.delta", "pandas.csv"])
def binding_contract(request, monkeypatch):
    original = _bindings

    def selected():
        document = original()
        if request.param != "pandas.csv":
            binding = document["bindings"]["raw.customers"]
            binding.update(adapter=request.param, format="delta")
            binding["target"]["kind"] = "delta_table"
        return document

    monkeypatch.setattr(sys.modules[__name__], "_bindings", selected)


def _write(path, document):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _catalog(root, definitions=None, bindings=None, **options):
    definition_path = _write(root / "definitions.yaml", _definitions() if definitions is None else definitions)
    binding_path = _write(root / "bindings.yaml", _bindings() if bindings is None else bindings)
    return ManifestCatalog(
        project_root=root, definitions=[definition_path], bindings=[binding_path], environment="dev", **options
    )


def test_valid_catalog_separates_definitions_from_bindings(tmp_path):
    catalog = _catalog(tmp_path)
    snapshot = catalog.snapshot
    definition = snapshot.definitions["raw.customers"]
    assert definition.schema.columns["id"].type == "int64"
    assert definition.schema.extra_columns == "forbid"
    assert definition.constraints[0].columns == ("id",)
    assert not hasattr(definition, "adapter")
    assert not hasattr(definition, "target")
    assert snapshot.require_binding("raw.customers").target.path == tmp_path / "data" / "customers.csv"
    assert "virtual.customers" in snapshot.definitions
    assert "virtual.customers" not in snapshot.bindings
    assert not (tmp_path / "data").exists()


def test_snapshot_is_deeply_immutable(tmp_path):
    snapshot = _catalog(tmp_path).snapshot
    definition = snapshot.definitions["raw.customers"]
    with pytest.raises(TypeError):
        snapshot.definitions["other"] = definition
    with pytest.raises(TypeError):
        definition.schema.columns["id"] = None
    with pytest.raises(TypeError):
        definition.metadata["nested"]["values"] = ()
    assert definition.metadata["nested"]["values"] == (1, True, None, "note")
    with pytest.raises(FrozenInstanceError):
        definition.schema.columns["id"].nullable = True
    with pytest.raises(TypeError):
        snapshot.bindings["raw.customers"].options["header"] = False


@pytest.mark.parametrize("field", ["engine", "storage_path", "adapter", "options", "unexpected"])
def test_physical_or_unknown_logical_fields_are_rejected(tmp_path, field):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"][field] = "not-allowed"
    with pytest.raises(CatalogParseError, match=field):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize("version", [True, "1", 1.0, 0, 2, None])
def test_schema_version_is_exact_supported_integer(tmp_path, version):
    definitions = _definitions()
    definitions["schema_version"] = version
    with pytest.raises(CatalogParseError, match="schema_version"):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize("data_type", ["integer", "date", "timestamp", "decimal", "object", ["string"]])
def test_unsupported_column_types_fail_explicitly(tmp_path, data_type):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"]["schema"]["columns"]["name"]["type"] = data_type
    with pytest.raises(CatalogParseError, match="columns/name/type"):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize("nullable", ["false", 0, None])
def test_nullability_requires_boolean(tmp_path, nullable):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"]["schema"]["columns"]["name"]["nullable"] = nullable
    with pytest.raises(CatalogParseError, match="nullable"):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize(
    "field,value",
    [("schema", {}), ("constraints", {}), ("governance", {"access": "admin"}), ("metadata", {"number": float("nan")})],
)
def test_invalid_nested_declarations_fail(tmp_path, field, value):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"][field] = value
    with pytest.raises(CatalogParseError, match=field):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize("dataset_id", ["", "raw..customers", "raw/customers", "123customers", 123, True])
def test_dataset_id_grammar_is_explicit(tmp_path, dataset_id):
    definitions = _definitions()
    definitions["datasets"] = {dataset_id: definitions["datasets"]["raw.customers"]}
    with pytest.raises(CatalogParseError):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize(
    "update",
    [
        {"type": "not_null"},
        {"columns": ["missing"]},
        {"columns": ["name"]},
        {"columns": ["id", "id"]},
        {"columns": []},
        {"severity": "warning"},
        {"enabled": False},
    ],
)
def test_unsupported_or_invalid_constraints_fail(tmp_path, update):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"]["constraints"][0].update(update)
    with pytest.raises(CatalogParseError, match="constraints"):
        _catalog(tmp_path, definitions)


def test_duplicate_constraint_ids_fail(tmp_path):
    definitions = _definitions()
    constraints = definitions["datasets"]["raw.customers"]["constraints"]
    constraints.append(copy.deepcopy(constraints[0]))
    with pytest.raises(CatalogParseError, match="Duplicate constraint"):
        _catalog(tmp_path, definitions)


@pytest.mark.parametrize(
    "field,value",
    [
        ("adapter", "spark.csv"),
        ("format", "parquet"),
        ("target", {"kind": "uri", "uri": "s3://bucket/table"}),
        ("options", {"header": False}),
        ("access", "overwrite"),
        ("mode", "overwrite"),
    ],
)
def test_unsupported_bindings_fail(tmp_path, field, value):
    bindings = _bindings()
    bindings["bindings"]["raw.customers"][field] = value
    with pytest.raises(CatalogParseError, match=field):
        _catalog(tmp_path, bindings=bindings)


@pytest.mark.parametrize(
    "target",
    [
        "/tmp/table.csv",
        "../table.csv",
        "data/../table.csv",
        "s3://bucket/x",
        "C:\\x.csv",
        "~/.csv",
        "data//x.csv",
        "data/bad\x00.csv",
        "data/bad\n.csv",
    ],
)
def test_only_portable_project_relative_targets_are_allowed(tmp_path, target):
    bindings = _bindings()
    bindings["bindings"]["raw.customers"]["target"]["path"] = target
    with pytest.raises(CatalogParseError, match="target/path"):
        _catalog(tmp_path, bindings=bindings)


def test_binding_environment_and_references_are_checked(tmp_path):
    bindings = _bindings()
    bindings["environment"] = "prod"
    with pytest.raises(CatalogParseError, match="environment"):
        _catalog(tmp_path, bindings=bindings)
    bindings = _bindings()
    bindings["bindings"]["missing"] = bindings["bindings"].pop("raw.customers")
    with pytest.raises(CatalogParseError, match="logical definition"):
        _catalog(tmp_path, bindings=bindings)


def test_binding_resolution_checks_access_without_io(tmp_path):
    snapshot = _catalog(tmp_path).snapshot
    with pytest.raises(CatalogParseError, match="read_only"):
        snapshot.require_binding("raw.customers", access="write")
    with pytest.raises(CatalogParseError, match="binding"):
        snapshot.require_binding("virtual.customers")
    with pytest.raises(CatalogParseError, match="Unknown dataset"):
        snapshot.require_binding("missing")


def test_interpolation_uses_only_explicit_variables(tmp_path, monkeypatch):
    bindings = _bindings()
    bindings["bindings"]["raw.customers"]["target"]["path"] = "${DATA_ROOT}/customers.csv"
    monkeypatch.setenv("DATA_ROOT", "unexpected")
    with pytest.raises(CatalogParseError, match="target/path"):
        _catalog(tmp_path, bindings=bindings)
    catalog = _catalog(tmp_path, bindings=bindings, variables={"DATA_ROOT": "data/dev"})
    assert catalog.snapshot.require_binding("raw.customers").target.path == tmp_path / "data/dev/customers.csv"


@pytest.mark.parametrize("value", ["${UNDECLARED}", "${malformed-name}", "prefix-${"])
def test_logical_interpolation_is_rejected(tmp_path, value):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"]["description"] = value
    with pytest.raises(CatalogParseError, match="description"):
        _catalog(tmp_path, definitions)


def test_field_provenance_identifies_file_and_location(tmp_path):
    snapshot = _catalog(tmp_path).snapshot
    source = snapshot.sources[tmp_path / "definitions.yaml"][
        ("datasets", "raw.customers", "schema", "columns", "id", "type")
    ]
    assert source.path == tmp_path / "definitions.yaml"
    assert source.line > 1
    assert source.column > 1


@pytest.mark.parametrize("manifest_kind", ["definitions", "bindings"])
def test_duplicate_ids_across_explicit_files_report_both_sources(tmp_path, manifest_kind):
    catalog = _catalog(tmp_path)
    first = tmp_path / f"{manifest_kind}.yaml"
    second = _write(tmp_path / "second.yaml", _definitions() if manifest_kind == "definitions" else _bindings())
    selected = {
        "definitions": [tmp_path / "definitions.yaml"],
        "bindings": [tmp_path / "bindings.yaml"],
    }
    selected[manifest_kind].append(second)
    with pytest.raises(CatalogParseError) as captured:
        ManifestCatalog(project_root=tmp_path, environment="dev", **selected)
    assert str(first) in str(captured.value)
    assert str(second) in str(captured.value)
    assert catalog.snapshot.definitions["raw.customers"].id == "raw.customers"


@pytest.mark.parametrize(
    "contents",
    [
        "schema_version: 1\nschema_version: 1\n",
        "schema_version: 1\nkind: DatasetDefinitions\ndatasets:\n"
        "  x:\n    schema:\n      nullable: true\n      nullable: false\n",
        "schema_version: 1\nkind: DatasetDefinitions\ndatasets: &datasets {}\n",
        "schema_version: 1\nkind: DatasetDefinitions\ndatasets: *unknown\n",
        "schema_version: 1\nkind: DatasetDefinitions\ndatasets:\n  <<: {}\n",
        "!!python/object/apply:os.system ['must-not-run']",
        "[]",
        "",
        "---\n{}\n---\n{}",
    ],
)
def test_ambiguous_or_unsafe_yaml_is_rejected(tmp_path, contents):
    manifest = tmp_path / "invalid.yaml"
    manifest.write_text(contents, encoding="utf-8")
    with patch("os.system") as command:
        with pytest.raises(CatalogParseError):
            ManifestCatalog(project_root=tmp_path, definitions=[manifest], bindings=[], environment="dev")
    command.assert_not_called()


def test_failed_reload_keeps_identical_snapshot(tmp_path):
    catalog = _catalog(tmp_path)
    previous = catalog.snapshot
    _write(tmp_path / "definitions.yaml", _definitions())
    _write(tmp_path / "bindings.yaml", {**_bindings(), "unexpected": True})
    with pytest.raises(CatalogParseError):
        catalog.reload()
    assert catalog.snapshot is previous
    _write(tmp_path / "bindings.yaml", _bindings())
    updated = catalog.reload()
    assert updated is catalog.snapshot
    assert updated is not previous


def test_selected_paths_are_explicit_and_missing_files_fail(tmp_path):
    _write(tmp_path / "definitions.yaml", _definitions())
    (tmp_path / "unselected.yaml").write_text("malformed: [", encoding="utf-8")
    catalog = ManifestCatalog(project_root=tmp_path, definitions=["definitions.yaml"], bindings=[], environment="dev")
    assert len(catalog.snapshot.definitions) == 2
    with pytest.raises(CatalogParseError, match="missing.yaml"):
        ManifestCatalog(project_root=tmp_path, definitions=["missing.yaml"], bindings=[], environment="dev")
    with pytest.raises(CatalogParseError):
        ManifestCatalog(project_root=tmp_path, definitions="definitions.yaml", bindings=[], environment="dev")


def test_projects_with_same_id_have_independent_snapshots(tmp_path):
    first = _catalog(tmp_path / "first").snapshot
    second = _catalog(tmp_path / "second").snapshot
    assert first.require_binding("raw.customers").target.path != second.require_binding("raw.customers").target.path
    assert first.definitions["raw.customers"] is not second.definitions["raw.customers"]


def test_errors_do_not_echo_invalid_values(tmp_path):
    definitions = _definitions()
    definitions["datasets"]["raw.customers"]["schema"]["columns"]["id"]["type"] = "sensitive-invalid-value"
    with pytest.raises(CatalogParseError) as captured:
        _catalog(tmp_path, definitions)
    message = str(captured.value)
    assert "sensitive-invalid-value" not in message
    assert str(tmp_path / "definitions.yaml") in message
    assert "columns/id/type" in message


def test_metadata_loads_without_optional_engines(tmp_path):
    _catalog(tmp_path)
    package_root = Path(__file__).resolve().parents[2]
    script = """
import builtins
from pathlib import Path
original_import = builtins.__import__
def metadata_only(name, *args, **kwargs):
    if name.split(".")[0] in ("pandas", "pyspark", "pyarrow", "deltalake"):
        raise ImportError("Optional dependency unavailable")
    return original_import(name, *args, **kwargs)
builtins.__import__ = metadata_only
from flint_core.core.catalog.v1 import ManifestCatalog
catalog = ManifestCatalog(
    project_root=Path.cwd(), definitions=["definitions.yaml"], bindings=["bindings.yaml"], environment="dev"
)
assert "raw.customers" in catalog.snapshot.definitions
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(package_root)},
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_metadata_never_opens_or_stats_dataset_targets(tmp_path):
    _catalog(tmp_path)
    target_dir = tmp_path / "data"
    original_open = Path.open
    original_stat = Path.stat

    def guarded_open(path, *args, **kwargs):
        assert path != target_dir and target_dir not in path.parents
        return original_open(path, *args, **kwargs)

    def guarded_stat(path, *args, **kwargs):
        assert path != target_dir and target_dir not in path.parents
        return original_stat(path, *args, **kwargs)

    with patch.object(Path, "open", guarded_open), patch.object(Path, "stat", guarded_stat):
        catalog = ManifestCatalog(
            project_root=tmp_path, definitions=["definitions.yaml"], bindings=["bindings.yaml"], environment="dev"
        )
        assert catalog.snapshot.require_binding("raw.customers").target.path == target_dir / "customers.csv"


@pytest.mark.parametrize("limit", ["bytes", "depth", "nodes"])
def test_yaml_resource_limits_are_enforced(tmp_path, monkeypatch, limit):
    from flint_core.core.catalog.v1 import yaml_loader

    manifest = _write(tmp_path / "definitions.yaml", _definitions())
    if limit == "bytes":
        monkeypatch.setattr(yaml_loader, "_MAX_BYTES", 32)
    elif limit == "depth":
        manifest.write_text("[" * 65 + "0" + "]" * 65, encoding="utf-8")
    else:
        monkeypatch.setattr(yaml_loader, "_MAX_NODES", 10)
    with pytest.raises(CatalogParseError, match="limit"):
        ManifestCatalog(project_root=tmp_path, definitions=[manifest], bindings=[], environment="dev")


def test_invalid_encoding_is_a_declaration_error(tmp_path):
    manifest = tmp_path / "definitions.yaml"
    manifest.write_bytes(b"\xff")
    with pytest.raises(CatalogParseError, match="UTF-8"):
        ManifestCatalog(project_root=tmp_path, definitions=[manifest], bindings=[], environment="dev")


def test_yaml_reader_errors_are_sanitized_declaration_errors(tmp_path):
    manifest = tmp_path / "definitions.yaml"
    manifest.write_text("secret: sensitive-value\x00\n", encoding="utf-8")
    with pytest.raises(CatalogParseError) as captured:
        ManifestCatalog(project_root=tmp_path, definitions=[manifest], bindings=[], environment="dev")
    assert "sensitive-value" not in str(captured.value)


def test_duplicate_yaml_locations_do_not_echo_values(tmp_path):
    manifest = tmp_path / "definitions.yaml"
    manifest.write_text("schema_version: first-secret\nschema_version: second-secret\n", encoding="utf-8")
    with pytest.raises(CatalogParseError) as captured:
        ManifestCatalog(project_root=tmp_path, definitions=[manifest], bindings=[], environment="dev")
    assert f"{manifest}:1:1" in str(captured.value)
    assert f"{manifest}:2:1" in str(captured.value)
    assert "first-secret" not in str(captured.value)
    assert "second-secret" not in str(captured.value)


def test_variables_are_copied_and_revalidated_after_substitution(tmp_path):
    bindings = _bindings()
    bindings["bindings"]["raw.customers"]["target"]["path"] = "${ROOT}/customers.csv"
    variables = {"ROOT": "data"}
    catalog = _catalog(tmp_path, bindings=bindings, variables=variables)
    variables["ROOT"] = "../escaped"
    catalog.reload()
    assert catalog.snapshot.require_binding("raw.customers").target.path == tmp_path / "data/customers.csv"
    with pytest.raises(CatalogParseError, match="target/path"):
        _catalog(tmp_path, bindings=bindings, variables=variables)


def test_only_explicit_empty_dataset_mapping_clears_snapshot(tmp_path):
    catalog = _catalog(tmp_path)
    _write(tmp_path / "definitions.yaml", {"schema_version": 1, "kind": "DatasetDefinitions", "datasets": {}})
    _write(tmp_path / "bindings.yaml", {**_bindings(), "bindings": {}})
    assert catalog.reload().definitions == {}


def test_checked_in_examples_match_the_implemented_contract():
    root = Path(__file__).resolve().parents[2]
    catalog = ManifestCatalog(
        project_root=root,
        definitions=["docs/examples/catalog-v1/definitions.yaml"],
        bindings=["docs/examples/catalog-v1/bindings.dev.yaml"],
        environment="dev",
    )
    assert set(catalog.snapshot.definitions) == {"raw.customers", "intermediate.customer_ids"}
    assert set(catalog.snapshot.bindings) == {"raw.customers"}


def test_candidate_is_not_published_while_manifests_are_loading(tmp_path):
    from flint_core.core.catalog.v1 import loader

    catalog = _catalog(tmp_path)
    previous = catalog.snapshot
    read_document = loader.read_document
    visited = []

    def observe_candidate(path):
        result = read_document(path)
        visited.append(path.name)
        assert catalog.snapshot is previous
        return result

    with patch.object(loader, "read_document", side_effect=observe_candidate):
        updated = catalog.reload()
    assert visited == ["definitions.yaml", "bindings.yaml"]
    assert updated is not previous


def test_read_error_preserves_old_snapshot(tmp_path):
    catalog = _catalog(tmp_path)
    previous = catalog.snapshot
    original_open = Path.open

    def interrupted_open(path, *args, **kwargs):
        if path.name == "bindings.yaml":
            raise OSError("Simulated interrupted read")
        return original_open(path, *args, **kwargs)

    with patch.object(Path, "open", interrupted_open):
        with pytest.raises(CatalogParseError, match="bindings.yaml"):
            catalog.reload()
    assert catalog.snapshot is previous


def test_environment_binding_and_root_are_independent_of_cwd(tmp_path, monkeypatch):
    dev = _catalog(tmp_path)
    prod_bindings = _bindings()
    prod_bindings["environment"] = "prod"
    prod_bindings["bindings"]["raw.customers"]["target"]["path"] = "data/prod/customers.csv"
    _write(tmp_path / "bindings.prod.yaml", prod_bindings)
    monkeypatch.chdir(tmp_path.parent)
    prod = ManifestCatalog(
        project_root=tmp_path, definitions=["definitions.yaml"], bindings=["bindings.prod.yaml"], environment="prod"
    )
    assert dev.snapshot.environment == "dev"
    assert prod.snapshot.environment == "prod"
    assert dev.snapshot.require_binding("raw.customers").target.path == tmp_path / "data/customers.csv"
    assert prod.snapshot.require_binding("raw.customers").target.path == tmp_path / "data/prod/customers.csv"
