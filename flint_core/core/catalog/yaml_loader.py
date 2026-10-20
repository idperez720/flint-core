"""Safe catalog YAML loading without implicit key replacement."""

from pathlib import Path
from typing import Any, Dict, Tuple

import yaml

from flint_core.core.exceptions import CatalogParseError


def source_location(mark: yaml.Mark) -> str:
    """Formats a declaration location without including source values."""
    return f"{mark.name}:{mark.line + 1}:{mark.column + 1} (line {mark.line + 1}, column {mark.column + 1})"


class _CatalogLoader(yaml.SafeLoader):
    """Rejects explicit duplicates and collisions introduced by YAML merges."""

    def _check_keys(self, node: yaml.MappingNode) -> None:
        declarations: Dict[Any, yaml.Mark] = {}
        for key_node, _ in node.value:
            key = "<<" if key_node.tag == "tag:yaml.org,2002:merge" else self.construct_object(key_node)
            try:
                previous = declarations.get(key)
                if previous is not None:
                    raise CatalogParseError(
                        f"Duplicate YAML key {key!r}: first declared at {source_location(previous)}; "
                        f"repeated at {source_location(key_node.start_mark)}."
                    )
                declarations[key] = key_node.start_mark
            except TypeError as error:
                raise CatalogParseError(
                    f"Unhashable YAML mapping key at {source_location(key_node.start_mark)}."
                ) from error

    def flatten_mapping(self, node: yaml.MappingNode) -> None:
        """Checks keys before merge markers disappear and after inheritance."""
        self._check_keys(node)
        super().flatten_mapping(node)
        self._check_keys(node)


def load_manifest(path: Path) -> Tuple[Any, Dict[str, yaml.Mark]]:
    """Loads metadata and root declaration marks using only safe constructors."""
    try:
        with path.open("r", encoding="utf-8") as stream:
            loader = _CatalogLoader(stream)
            try:
                node = loader.get_single_node()
                content = loader.construct_document(node) if node is not None else None
                locations = (
                    {key_node.value: key_node.start_mark for key_node, _ in node.value}
                    if isinstance(node, yaml.MappingNode)
                    else {}
                )
                return content, locations
            finally:
                loader.dispose()
    except yaml.YAMLError as error:
        raise CatalogParseError(f"Invalid YAML in catalog manifest '{path}': {error}") from error
