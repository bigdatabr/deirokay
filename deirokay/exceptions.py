"""
Exception types for Deirokay processes.
"""

from .enums import SeverityLevel


class ValidationError(Exception):
    """Validation failure exception."""

    def __init__(self, level: SeverityLevel, message='Validation failed'):
        self.level = level
        super().__init__(message)


class InvalidBackend(ValueError):
    """Error in backend definition."""


class InvalidBackend(InvalidBackend):
    """Invalid backend selection."""


class UnsupportedBackend(InvalidBackend):
    """Backend is not supported for this resource."""


class ParsingError(RuntimeError):
    """Error during file parsing or object conversion."""


class InvalidStatement(SyntaxError):
    """Error when validating statement construction."""
