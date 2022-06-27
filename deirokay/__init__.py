"""
Deirokay's main set of classes and functions.
"""

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    import lazy_import
    lazy_import.lazy_module('dask.dataframe')
    lazy_import.lazy_module('numpy')
    lazy_import.lazy_module('pandas')


from .parser import data_reader
from .profiler import profile
from .validator import validate

__all__ = (
    'data_reader',
    'profile',
    'validate',
)
