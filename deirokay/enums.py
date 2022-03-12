"""
Deirokay enumeration classes.
"""

from enum import Enum


class DTypes(str, Enum):
    """Deirokay data types."""

    INT64 = 'integer'
    """Integer values, treated by
    :ref:`IntegerTreater<deirokay.parser.treaters.IntegerTreater>`."""
    INTEGER = INT64
    """Alias for INT64."""
    FLOAT64 = 'float'
    """Float values, treated by
    :ref:`FloatTreater<deirokay.parser.treaters.FloatTreater>`."""
    FLOAT = FLOAT64
    """Alias for FLOAT64."""
    STRING = 'string'
    """Text values, treated by
    :ref:`StringTreater<deirokay.parser.treaters.StringTreater>`."""
    STR = STRING
    """Alias for STRING."""
    DATETIME = 'datetime'
    """Datetime values, treated by
    :ref:`DateTime64Treater<deirokay.parser.treaters.DateTime64Treater>`."""
    DT = DATETIME
    """Alias for DATETIME."""
    DATE = 'date'
    """Date values, treated by
    :ref:`DateTreater<deirokay.parser.treaters.DateTreater>`."""
    TIME = 'time'
    """Time values, treated by
    :ref:`TimeTreater<deirokay.parser.treaters.TimeTreater>`."""
    BOOLEAN = 'boolean'
    """Boolean values, treated by
    :ref:`BooleanTreater<deirokay.parser.treaters.BooleanTreater>`."""
    BOOL = BOOLEAN
    """Alias for BOOLEAN."""
    DECIMAL = 'decimal'
    """Decimal values, treated by
    :ref:`DecimalTreater<deirokay.parser.treaters.DecimalTreater>`."""


class SeverityLevel(int, Enum):
    """Deirokay named Severity levels."""

    MINIMAL = 1
    """Minimal severity validation error."""
    WARNING = 3
    """Validation warning."""
    CRITICAL = 5
    """Critical validation error."""
