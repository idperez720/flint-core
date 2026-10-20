"""Bounded safe YAML parsing with field-level provenance and no aliases or merges."""

from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Dict, NoReturn

import yaml

from flint_core.core.catalog.v1.models import FieldPath, SourceLocation
from flint_core.core.exceptions import CatalogParseError

_MAX_BYTES = 1024 * 1024
_MAX_DEPTH = 64
_MAX_NODES = 100_000


def _location(mark: yaml.Mark) -> SourceLocation:
    return SourceLocation(Path(mark.name), mark.line + 1, mark.column + 1)


class _StrictLoader(yaml.SafeLoader):
    def __init__(self, stream: StringIO) -> None:
        self._depth = 0
        self._node_count = 0
        super().__init__(stream)

    def compose_node(self, parent: Any, index: Any) -> yaml.Node:
        event = self.peek_event()
        if self.check_event(yaml.AliasEvent) or getattr(event, "anchor", None) is not None:
            raise CatalogParseError(f"{_location(event.start_mark)}: YAML aliases and anchors are not supported in v1.")
        self._depth += 1
        self._node_count += 1
        try:
            if self._depth > _MAX_DEPTH or self._node_count > _MAX_NODES:
                raise CatalogParseError(f"{_location(event.start_mark)}: YAML structure exceeds the v1 safety limits.")
            return super().compose_node(parent, index)
        finally:
            self._depth -= 1


@dataclass
class Document:
    path: Path
    data: Any
    sources: Dict[FieldPath, SourceLocation]

    def source(self, field: FieldPath) -> SourceLocation:
        while field not in self.sources:
            field = field[:-1]
        return self.sources[field]

    def fail(self, field: FieldPath, message: str) -> NoReturn:
        pointer = "/".join(str(part).replace("~", "~0").replace("/", "~1") for part in field) or "/"
        raise CatalogParseError(f"{self.source(field)} [{pointer}]: {message}")


def _collect_sources(node: yaml.Node, field: FieldPath, sources: Dict[FieldPath, SourceLocation]) -> None:
    sources.setdefault(field, _location(node.start_mark))
    if isinstance(node, yaml.MappingNode):
        declarations: Dict[str, SourceLocation] = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                raise CatalogParseError(f"{_location(key_node.start_mark)}: YAML merges are not supported in v1.")
            if not isinstance(key_node, yaml.ScalarNode) or key_node.tag != "tag:yaml.org,2002:str":
                raise CatalogParseError(f"{_location(key_node.start_mark)}: YAML mapping keys must be strings.")
            key = key_node.value
            location = _location(key_node.start_mark)
            if key in declarations:
                raise CatalogParseError(
                    f"Duplicate YAML key: first declared at {declarations[key]}; repeated at {location}."
                )
            declarations[key] = location
            sources[field + (key,)] = location
            _collect_sources(value_node, field + (key,), sources)
    elif isinstance(node, yaml.SequenceNode):
        for position, item in enumerate(node.value):
            _collect_sources(item, field + (position,), sources)


def read_document(path: Path) -> Document:
    try:
        if not path.is_file() or path.suffix not in (".yaml", ".yml"):
            raise CatalogParseError(f"{path}: Selected manifest must be an existing .yaml or .yml file.")
        with path.open("rb") as stream:
            raw = stream.read(_MAX_BYTES + 1)
        if len(raw) > _MAX_BYTES:
            raise CatalogParseError(f"{path}: Manifest exceeds the v1 size limit.")
        contents = raw.decode("utf-8-sig")
    except (OSError, UnicodeError):
        raise CatalogParseError(f"{path}: Cannot read selected manifest as UTF-8.") from None
    stream = StringIO(contents)
    stream.name = str(path)
    loader = None
    try:
        loader = _StrictLoader(stream)
        node = loader.get_single_node()
        if node is None:
            raise CatalogParseError(f"{path}: Empty manifests are not supported in v1.")
        sources: Dict[FieldPath, SourceLocation] = {}
        _collect_sources(node, (), sources)
        return Document(path, loader.construct_document(node), sources)
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        location = _location(mark) if mark is not None else path
        raise CatalogParseError(f"{location}: Invalid or unsupported YAML.") from None
    finally:
        if loader is not None:
            loader.dispose()
        stream.close()
