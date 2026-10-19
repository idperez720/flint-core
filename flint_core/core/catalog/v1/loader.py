"""Strict declaration validation and atomic publication of opt-in v1 catalogs."""

import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from typing import Any, Dict, Optional, Tuple, Union

from flint_core.core.catalog.v1.models import (
    CatalogSnapshot,
    ColumnDefinition,
    DatasetBinding,
    DatasetDefinition,
    DatasetSchema,
    DeltaTableTarget,
    FieldPath,
    FileTarget,
    UniqueConstraint,
)
from flint_core.core.catalog.v1.yaml_loader import Document, read_document
from flint_core.core.exceptions import CatalogParseError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_ENVIRONMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
_VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_TYPES = frozenset({"string", "boolean", "int64", "float64"})


def _mapping(document: Document, value: Any, field: FieldPath, required=(), optional=()) -> dict:
    if not isinstance(value, dict):
        document.fail(field, "Expected a mapping.")
    for key in required:
        if key not in value:
            document.fail(field + (key,), "Required field is missing.")
    allowed = set(required) | set(optional)
    for key in value:
        if key not in allowed:
            document.fail(field + (key,), "Unknown field.")
    return value


def _text(document: Document, value: Any, field: FieldPath) -> str:
    if not isinstance(value, str) or not value.strip():
        document.fail(field, "Expected a non-empty string.")
    if "${" in value:
        document.fail(field, "Interpolation is permitted only in physical target.path.")
    return value


def _choice(document: Document, value: Any, field: FieldPath, allowed) -> str:
    value = _text(document, value, field)
    if value not in allowed:
        document.fail(field, "Unsupported value for this field.")
    return value


def _identifier(document: Document, value: Any, field: FieldPath) -> str:
    value = _text(document, value, field)
    if _IDENTIFIER.fullmatch(value) is None:
        document.fail(field, "Expected an explicit dot-separated identifier.")
    return value


def _json_value(document: Document, value: Any, field: FieldPath) -> Any:
    if value is None or type(value) in (bool, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, str):
        if "${" in value:
            document.fail(field, "Interpolation is permitted only in physical target.path.")
        return value
    if isinstance(value, list):
        return tuple(_json_value(document, item, field + (position,)) for position, item in enumerate(value))
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if "${" in key:
                document.fail(field + (key,), "Interpolation is not permitted in keys.")
            result[key] = _json_value(document, item, field + (key,))
        return MappingProxyType(result)
    document.fail(field, "Expected finite JSON-compatible annotation data.")


def _schema(document: Document, value: Any, field: FieldPath) -> DatasetSchema:
    data = _mapping(document, value, field, required=("columns", "extra_columns"))
    policy = _choice(document, data["extra_columns"], field + ("extra_columns",), ("allow", "forbid"))
    raw_columns = data["columns"]
    if not isinstance(raw_columns, dict) or not raw_columns:
        document.fail(field + ("columns",), "Expected a non-empty column mapping.")
    columns = {}
    for name, raw_column in raw_columns.items():
        column_field = field + ("columns", name)
        _text(document, name, column_field)
        column = _mapping(document, raw_column, column_field, required=("type", "nullable"), optional=("description",))
        data_type = _choice(document, column["type"], column_field + ("type",), _TYPES)
        nullable = column["nullable"]
        if type(nullable) is not bool:
            document.fail(column_field + ("nullable",), "Expected a YAML boolean.")
        description = (
            _text(document, column["description"], column_field + ("description",)) if "description" in column else None
        )
        columns[name] = ColumnDefinition(data_type, nullable, description, document.source(column_field))
    return DatasetSchema(MappingProxyType(columns), policy)


def _constraints(
    document: Document, value: Any, field: FieldPath, schema: DatasetSchema
) -> Tuple[UniqueConstraint, ...]:
    if not isinstance(value, list):
        document.fail(field, "Expected a constraint list.")
    result = []
    declarations = {}
    for position, raw_constraint in enumerate(value):
        rule_field = field + (position,)
        rule = _mapping(document, raw_constraint, rule_field, required=("id", "type", "columns", "severity"))
        rule_id = _identifier(document, rule["id"], rule_field + ("id",))
        if rule_id in declarations:
            document.fail(rule_field + ("id",), f"Duplicate constraint ID; first declared at {declarations[rule_id]}.")
        declarations[rule_id] = document.source(rule_field + ("id",))
        _choice(document, rule["type"], rule_field + ("type",), ("unique",))
        severity = _choice(document, rule["severity"], rule_field + ("severity",), ("error",))
        columns = rule["columns"]
        if not isinstance(columns, list) or not columns:
            document.fail(rule_field + ("columns",), "Expected a non-empty column list.")
        seen = set()
        for index, name in enumerate(columns):
            column_field = rule_field + ("columns", index)
            _text(document, name, column_field)
            if name not in schema.columns or name in seen:
                document.fail(column_field, "Constraint columns must be distinct declared columns.")
            if schema.columns[name].nullable:
                document.fail(column_field, "Unique constraint columns must declare nullable: false.")
            seen.add(name)
        result.append(UniqueConstraint(rule_id, tuple(columns), severity, document.source(rule_field)))
    return tuple(result)


def _definition(document: Document, dataset_id: str, value: Any) -> DatasetDefinition:
    field = ("datasets", dataset_id)
    _identifier(document, dataset_id, field)
    data = _mapping(
        document, value, field, required=("schema",), optional=("description", "constraints", "governance", "metadata")
    )
    schema = _schema(document, data["schema"], field + ("schema",))
    description = _text(document, data["description"], field + ("description",)) if "description" in data else None
    governance = _mapping(
        document,
        data.get("governance", {}),
        field + ("governance",),
        optional=("owner", "domain", "classification", "tags"),
    )
    for key, item in governance.items():
        if key == "tags":
            if not isinstance(item, list):
                document.fail(field + ("governance", key), "Expected a string list.")
            for position, tag in enumerate(item):
                _text(document, tag, field + ("governance", key, position))
        else:
            _text(document, item, field + ("governance", key))
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        document.fail(field + ("metadata",), "Expected an annotation mapping.")
    return DatasetDefinition(
        dataset_id,
        schema,
        description,
        _constraints(document, data.get("constraints", []), field + ("constraints",), schema),
        _json_value(document, governance, field + ("governance",)),
        _json_value(document, metadata, field + ("metadata",)),
        document.source(field),
    )


def _target(document: Document, value: Any, field: FieldPath, root: Path, variables: Mapping[str, str], *, delta=False):
    data = _mapping(document, value, field, required=("kind", "path"))
    _choice(document, data["kind"], field + ("kind",), ("delta_table",) if delta else ("file",))
    raw_path = data["path"]
    if not isinstance(raw_path, str) or not raw_path.strip():
        document.fail(field + ("path",), "Expected a non-empty project-relative file path.")

    def substitute(match: re.Match) -> str:
        if match.group(1) not in variables:
            document.fail(field + ("path",), "Unresolved explicit variable.")
        return variables[match.group(1)]

    path = _PLACEHOLDER.sub(substitute, raw_path)
    parts = path.split("/")
    if (
        "${" in path
        or ":" in path
        or "\\" in path
        or path.startswith("~")
        or path != path.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in path)
        or any(part in ("", ".", "..") for part in parts)
    ):
        document.fail(field + ("path",), "Expected a portable project-relative file path without traversal.")
    if delta and any(character in str(root / path) for character in ("%", "?", "#")):
        document.fail(
            field + ("path",), "Delta paths and project roots cannot contain ambiguous URI characters %, ?, or #."
        )
    target_type = DeltaTableTarget if delta else FileTarget
    return target_type(root.joinpath(*parts))


def _binding(document: Document, dataset_id: str, value: Any, root: Path, variables) -> DatasetBinding:
    field = ("bindings", dataset_id)
    _identifier(document, dataset_id, field)
    data = _mapping(document, value, field, required=("adapter", "format", "target", "access"), optional=("options",))
    adapter = _choice(document, data["adapter"], field + ("adapter",), ("pandas.csv", "pandas.delta", "spark.delta"))
    delta = adapter.endswith(".delta")
    data_format = _choice(document, data["format"], field + ("format",), ("delta",) if delta else ("csv",))
    access = _choice(document, data["access"], field + ("access",), ("read_only", "read_write"))
    options = _mapping(document, data.get("options", {}), field + ("options",), optional=("version",) if delta else ())
    if "version" in options:
        if type(options["version"]) is not int or options["version"] < 0:
            document.fail(field + ("options", "version"), "Expected a non-negative integer Delta version.")
        if access != "read_only":
            document.fail(field + ("options", "version"), "A historical binding must be read_only.")
    return DatasetBinding(
        dataset_id,
        adapter,
        data_format,
        _target(document, data["target"], field + ("target",), root, variables, delta=delta),
        access,
        MappingProxyType(options.copy()),
        document.source(field),
    )


def _envelope(document: Document, kind: str) -> dict:
    fields = (
        ("schema_version", "kind", "datasets")
        if kind == "DatasetDefinitions"
        else ("schema_version", "kind", "environment", "bindings")
    )
    data = _mapping(document, document.data, (), required=fields)
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        document.fail(("schema_version",), "Unsupported manifest version; expected integer 1.")
    _choice(document, data["kind"], ("kind",), (kind,))
    return data


class ManifestCatalog:
    """Loads the explicit minimum v1 metadata contract without dataset access."""

    def __init__(
        self,
        *,
        project_root: Union[str, Path],
        definitions: Sequence[Union[str, Path]],
        bindings: Sequence[Union[str, Path]],
        environment: str,
        variables: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._root = Path(project_root).resolve()
        if not self._root.is_dir():
            raise CatalogParseError(f"{self._root}: project_root must be an existing directory.")
        if not isinstance(environment, str) or _ENVIRONMENT.fullmatch(environment) is None:
            raise CatalogParseError("[environment]: Expected an explicit environment identifier.")
        self._environment = environment
        self._definitions = self._paths(definitions)
        self._bindings = self._paths(bindings)
        if not self._definitions:
            raise CatalogParseError("[definitions]: Select at least one definition manifest.")
        if variables is not None and not isinstance(variables, Mapping):
            raise CatalogParseError("[variables]: Expected an explicit string mapping.")
        copied_variables = dict(variables) if variables is not None else {}
        for name, value in copied_variables.items():
            if (
                not isinstance(name, str)
                or _VARIABLE.fullmatch(name) is None
                or not isinstance(value, str)
                or not value.strip()
                or "${" in value
            ):
                raise CatalogParseError("[variables]: Expected named, non-empty string values without interpolation.")
        self._variables = MappingProxyType(copied_variables)
        self._lock = RLock()
        self._snapshot = self._build_snapshot()

    def _paths(self, paths: Sequence[Union[str, Path]]) -> Tuple[Path, ...]:
        if isinstance(paths, (str, bytes)) or not isinstance(paths, Sequence):
            raise CatalogParseError("Manifest paths must be an explicit ordered sequence.")
        result = []
        for path in paths:
            if not isinstance(path, (str, Path)):
                raise CatalogParseError("Each selected manifest path must be a string or Path.")
            selected = Path(path)
            result.append((self._root / selected).resolve())
        return tuple(result)

    def _build_snapshot(self) -> CatalogSnapshot:
        definitions: Dict[str, DatasetDefinition] = {}
        bindings: Dict[str, DatasetBinding] = {}
        sources = {}
        for path in self._definitions:
            document = read_document(path)
            data = _envelope(document, "DatasetDefinitions")
            datasets = data["datasets"]
            if not isinstance(datasets, dict):
                document.fail(("datasets",), "Expected a dataset mapping.")
            for dataset_id, raw_definition in datasets.items():
                if dataset_id in definitions:
                    document.fail(
                        ("datasets", dataset_id),
                        f"Duplicate dataset ID; first declared at {definitions[dataset_id].source}.",
                    )
                definitions[dataset_id] = _definition(document, dataset_id, raw_definition)
            sources[path] = MappingProxyType(document.sources.copy())
        for path in self._bindings:
            document = read_document(path)
            data = _envelope(document, "DatasetBindings")
            environment = _text(document, data["environment"], ("environment",))
            if environment != self._environment:
                document.fail(("environment",), "Binding manifest does not match the selected environment.")
            raw_bindings = data["bindings"]
            if not isinstance(raw_bindings, dict):
                document.fail(("bindings",), "Expected a binding mapping.")
            for dataset_id, raw_binding in raw_bindings.items():
                if dataset_id not in definitions:
                    document.fail(("bindings", dataset_id), "Binding has no logical definition.")
                if dataset_id in bindings:
                    document.fail(
                        ("bindings", dataset_id),
                        f"Duplicate binding ID; first declared at {bindings[dataset_id].source}.",
                    )
                bindings[dataset_id] = _binding(document, dataset_id, raw_binding, self._root, self._variables)
            sources[path] = MappingProxyType(document.sources.copy())
        return CatalogSnapshot(
            self._root,
            self._environment,
            MappingProxyType(definitions),
            MappingProxyType(bindings),
            MappingProxyType(sources),
        )

    @property
    def snapshot(self) -> CatalogSnapshot:
        with self._lock:
            return self._snapshot

    def reload(self) -> CatalogSnapshot:
        """Publishes only a fully validated candidate; failures retain the old object."""
        with self._lock:
            candidate = self._build_snapshot()
            self._snapshot = candidate
            return candidate
