from enum import Enum


class DTypes(str, Enum):
    INT64 = 'integer'
    INTEGER = INT64
    FLOAT64 = 'float'
    FLOAT = FLOAT64
    STRING = 'string'
    STR = STRING
    DATETIME = 'datetime'
    DT = DATETIME
    DATE = DATETIME
    TIME = 'time'
    BOOLEAN = 'boolean'
    BOOL = BOOLEAN
