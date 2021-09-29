from enum import Enum


class DTypes(str, Enum):
    INT64 = 'Int64'
    FLOAT64 = 'Float64'
    STRING = 'string'
    STR = STRING
    DATETIME = 'datetime64[ns]'
    DT = DATETIME
    DATE = DATETIME
    TIME = 'time'
