"""
Deirokay enumeration classes.
"""

from enum import Enum


class DTypes(str, Enum):
    """Deirokay data types."""
    INT64 = 'integer'
    INTEGER = INT64
    FLOAT64 = 'float'
    FLOAT = FLOAT64
    STRING = 'string'
    STR = STRING
    DATETIME = 'datetime'
    DT = DATETIME
    DATE = 'date'
    TIME = 'time'
    BOOLEAN = 'boolean'
    BOOL = BOOLEAN
    DECIMAL = 'decimal'


class SeverityLevel(int, Enum):
    """Deirokay named Severity levels."""
    MINIMAL = 1
    WARNING = 3
    CRITICAL = 5
