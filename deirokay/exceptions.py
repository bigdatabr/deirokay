from .enums import Level


class ValidationError(Exception):
    def __init__(self, level: Level, message='Validation failed'):
        self.level = level
        super().__init__(message)
