class MomaError(Exception):
    """Base exception for all MoMa domain errors."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFoundError(MomaError):
    """Raised when a requested resource does not exist."""
    pass


class ConversionError(MomaError):
    """Raised when a Croissant profile cannot be converted to PG-JSON."""
    pass


class ValidationError(MomaError):
    """Raised when a PG-JSON structure fails schema validation."""
    pass


class RepositoryError(MomaError):
    """Raised when a repository operation fails unexpectedly."""
    pass
