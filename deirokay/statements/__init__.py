"""
Classes and methods to build and load builtin or custom Deirokay
statements.
"""
from typing import Dict, Type

from deirokay._utils import recursive_subclass_generator

from .builtin import BaseStatement

STATEMENTS_MAP: Dict[str, Type[BaseStatement]] = {
    cls.name: cls
    for cls in recursive_subclass_generator(BaseStatement)
}

__all__ = tuple(
    cls.__name__
    for cls in recursive_subclass_generator(BaseStatement)
)
