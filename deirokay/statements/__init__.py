"""
Module for BaseStatement and builtin Deirokay statements.
"""

import inspect

from .base_statement import BaseStatement
from .column_expression import ColumnExpression  # noqa F401
from .contain import Contain  # noqa F401
from .not_null import NotNull  # noqa F401
from .row_count import RowCount  # noqa F401
from .unique import Unique  # noqa F401

STATEMENTS_MAP = {
    cls.name: cls
    for _, cls in locals().items()
    if inspect.isclass(cls) and issubclass(cls, BaseStatement)
}

__all__ = tuple(
    cls.__name__ for cls in STATEMENTS_MAP.values()
)
