"""Opt-in v1 metadata catalog; legacy DataCatalog remains a separate API."""

from flint_core.core.catalog.v1.loader import ManifestCatalog
from flint_core.core.catalog.v1.models import CatalogSnapshot, DatasetBinding, DatasetDefinition, SourceLocation

__all__ = ["ManifestCatalog", "CatalogSnapshot", "DatasetDefinition", "DatasetBinding", "SourceLocation"]
