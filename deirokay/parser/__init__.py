"""
Classes and methods to load and parse correct data into Deirokay.
"""

from .loader import data_reader
from .treaters import get_dtype_treater, get_treater_instance

__all__ = (
    'data_reader',
    'get_dtype_treater',
    'get_treater_instance',
)
