"""Safe error categories for the versioned project workflow."""

from flint_core.core.exceptions import FlintError


class ProjectError(FlintError):
    """Invalid project configuration or trusted factory loading."""


class PlanError(FlintError):
    """A complete pipeline cannot be compiled safely."""


class DataValidationError(FlintError):
    """Dataset contents violate the declared contract."""

    def __init__(self, message, *, cleanup_pending=False):
        super().__init__(message)
        self.cleanup_pending = cleanup_pending


class PublicationConflict(FileExistsError):
    """A competing create won; a staged artifact may need manual cleanup."""

    def __init__(self, *, cleanup_pending=False):
        super().__init__("The create target already exists.")
        self.cleanup_pending = cleanup_pending


class PublicationUncertain(FlintError):
    """Publication was attempted but its durable outcome is unknown."""

    def __init__(self, *, cleanup_pending=False):
        super().__init__("Publication outcome is uncertain; do not retry automatically.")
        self.cleanup_pending = cleanup_pending
