"""Memory-optimized data entities for data catalog specifications."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union

from flint_core.core.catalog.adapters import AdapterRegistry
from flint_core.core.exceptions import ColumnValidationError


class ColumnDefinition:
    """Memory-isolated data model capturing explicit column specifications.

    Attributes:
        name: The physical identifier of the column.
        data_type: Structural semantic token mapping core representations.
        description: Optional internal documentation descriptor text.
        format: Custom chronological format layout string instructions.
        timezone: Target zone identifier forcing explicit localized shifts.
    """

    __slots__ = ("name", "data_type", "description", "format", "timezone")

    def __init__(
        self,
        name: str,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
        column_format: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> None:
        """Initializes a new declarative ColumnDefinition metadata entity."""
        self.name: str = name
        self.data_type: Optional[str] = data_type
        self.description: Optional[str] = description
        self.format: Optional[str] = column_format
        self.timezone: Optional[str] = timezone


class DatasetConfiguration:
    """High-performance structural entity tracking explicit dataset topologies.

    Manages dataset schema validation boundaries, runtime metadata options
    payload access, and lazy-loading evaluation context orchestration hooks.
    """

    __slots__ = (
        "name",
        "engine",
        "format",
        "storage_path",
        "connector",
        "columns",
        "metadata",
        "_column_names_set",
        "_catalog_ref",
    )

    def __init__(
        self,
        name: str,
        engine: str,
        data_format: str,
        storage_path: str,
        columns: List[ColumnDefinition],
        metadata: Dict[str, Any],
        connector: Optional[str] = None,
        catalog_ref: Optional[Any] = None,
    ) -> None:
        """Initializes a memory-isolated DatasetConfiguration data model."""
        self.name: str = name
        self.engine: str = engine
        self.format: str = data_format
        self.storage_path: str = storage_path
        self.connector: Optional[str] = connector
        self.columns: List[ColumnDefinition] = columns
        self.metadata: Dict[str, Any] = metadata
        self._column_names_set: Set[str] = {col.name for col in columns}
        self._catalog_ref: Optional[Any] = catalog_ref

    @property
    def column_names(self) -> List[str]:
        """Preserves precise declaration matrix sequencing ordering maps."""
        return [col.name for col in self.columns]

    def validate_schema(self, df: Any) -> bool:
        """Executes verification validation tests against metadata schemas.

        Args:
            df: Target polymorphic computational dataset DataFrame entity.

        Returns:
            bool: True if structural definitions evaluate identically.

        Raises:
            ColumnValidationError: If expected catalog columns are missing.
        """
        adapter = AdapterRegistry.resolve_adapter(df)
        actual_cols = adapter.extract_columns(df)
        missing_cols = self._column_names_set - actual_cols

        if missing_cols:
            raise ColumnValidationError(
                f"Schema validation failed for target '{self.name}'. "
                f"Missing expected catalog columns: {list(missing_cols)}"
            )
        return True

    def load(
        self,
        spark: Optional[Any] = None,
        version: Optional[Union[int, str]] = None,
        as_of: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Triggers fluid domain-driven data loading via attached parent catalog.

        Args:
            spark: Optional active distributed SparkSession manager reference.
            version: Optional target version index for Lakehouse time travel.
            as_of: Optional chronological timestamp snapshot constraint.
            options: Optional runtime reading property overrides dictionary.

        Returns:
            Any: The concrete computational DataFrame matrix instance.
        """
        if self._catalog_ref is None:
            raise RuntimeError(
                f"DatasetConfiguration entity '{self.name}' is detached from an active DataCatalog context."
            )
        return self._catalog_ref.load(
            self.name,
            spark=spark,
            version=version,
            as_of=as_of,
            options=options,
        )
