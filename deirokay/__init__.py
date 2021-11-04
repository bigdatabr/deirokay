"""
Gather main Deirokay methods to be easile accessible `from deirokay`.
"""

from .parser.parser import data_reader
from .profiler import profile
from .validator import validate

__all__ = (
    'data_reader',
    'profile',
    'validate',
)
