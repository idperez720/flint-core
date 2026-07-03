"""Centralized exception architecture for the flint framework."""


class FlintError(Exception):
    """Base exception for all errors raised by the flint framework."""

    pass


class UnsupportedBackendError(FlintError):
    """Raised when a dataframe backend is not supported or registered."""

    pass


class ColumnValidationError(FlintError):
    """Raised when dataframe columns fail schema or existence validations."""

    pass


class CatalogParseError(FlintError):
    """Raised when a YAML catalog file cannot be parsed due to syntax errors."""

    pass


class ProjectInitializationError(FlintError):
    """Raised when the project scaffolding pipeline encounters a failure."""

    pass
