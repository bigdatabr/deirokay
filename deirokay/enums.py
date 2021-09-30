from enum import Enum


class DTypes(str, Enum):
    INT64 = 'integer'
    FLOAT64 = 'float'
    STRING = 'string'
    STR = STRING
    DATETIME = 'datetime'
    DT = DATETIME
    DATE = DATETIME
    TIME = 'time'
