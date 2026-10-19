"""Immutable metadata models for the opt-in v1 catalog."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple, Union

from flint_core.core.exceptions import CatalogParseError

FieldPath = Tuple[Union[str, int], ...]


@dataclass(frozen=True, slots=True)
class SourceLocation:
    path: Path
    line: int
    column: int

    def __str__(self) -> str:
        return f"{self.path}:{self.line}:{self.column}"


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    type: str
    nullable: bool
    description: Optional[str]
    source: SourceLocation


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    columns: Mapping[str, ColumnDefinition]
    extra_columns: str


@dataclass(frozen=True, slots=True)
class UniqueConstraint:
    id: str
    columns: Tuple[str, ...]
    severity: str
    source: SourceLocation


@dataclass(frozen=True, slots=True)
class DatasetDefinition:
    id: str
    schema: DatasetSchema
    description: Optional[str]
    constraints: Tuple[UniqueConstraint, ...]
    governance: Mapping[str, Any]
    metadata: Mapping[str, Any]
    source: SourceLocation


@dataclass(frozen=True, slots=True)
class FileTarget:
    path: Path


@dataclass(frozen=True, slots=True)
class DeltaTableTarget:
    path: Path


@dataclass(frozen=True, slots=True)
class DatasetBinding:
    id: str
    adapter: str
    format: str
    target: Union[FileTarget, DeltaTableTarget]
    access: str
    options: Mapping[str, Any]
    source: SourceLocation


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    project_root: Path
    environment: str
    definitions: Mapping[str, DatasetDefinition]
    bindings: Mapping[str, DatasetBinding]
    sources: Mapping[Path, Mapping[FieldPath, SourceLocation]]

    def require_binding(self, dataset_id: str, *, access: str = "read") -> DatasetBinding:
        """Checks declarative access without touching the target or loading an engine."""
        if access not in ("read", "write"):
            raise ValueError("access must be 'read' or 'write'.")
        if dataset_id not in self.definitions:
            raise CatalogParseError(f"Unknown dataset ID {dataset_id!r}.")
        if dataset_id not in self.bindings:
            definition = self.definitions[dataset_id]
            raise CatalogParseError(f"{definition.source}: Dataset {dataset_id!r} requires a physical binding.")
        binding = self.bindings[dataset_id]
        if access == "write" and binding.access == "read_only":
            raise CatalogParseError(f"{binding.source}: Dataset {dataset_id!r} has read_only access.")
        return binding
