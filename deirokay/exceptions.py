from .enums import SeverityLevel


class ValidationError(Exception):
    def __init__(self, level: SeverityLevel, message='Validation failed'):
        self.level = level
        super().__init__(message)
