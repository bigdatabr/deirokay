import functools
from types import ModuleType
from typing import Type

from deirokay._typing import DeirokayStatement
from deirokay.backend import multibackend_class_factory
from deirokay.enums import Backend
from deirokay.fs import fs_factory
from deirokay.statements import BaseStatement

from . import STATEMENTS_MAP


@functools.lru_cache(maxsize=None)
def _cached_import_file_as_module(file_path: str) -> ModuleType:
    """Import a file as a module, caching the result.
    This prevents the need to read the same file multiple times.
    """
    fs = fs_factory(file_path)
    module = fs.import_as_python_module()
    return module


def _load_custom_statement(location: str) -> Type[BaseStatement]:
    """Load a custom statement from a .py file"""
    if '::' not in location:
        raise ValueError('You should pass your class location using the'
                         ' following pattern:\n'
                         '<.py file location>::<class name>')

    file_path, class_name = location.split('::')

    module = _cached_import_file_as_module(file_path)
    cls = getattr(module, class_name)

    if not issubclass(cls, BaseStatement):
        raise ImportError('Your custom statement should be a subclass of'
                          ' BaseStatement')

    return cls


def statement_factory(statement: DeirokayStatement,
                      backend: Backend) -> BaseStatement:
    """Receive statement dict and create the proper statement object.
    The `name` attribute of the class is used to bind the statement
    type to its class.

    Parameters
    ----------
    statement : dict
        Dict from Validation Document representing statement and its
        parameters.

    Returns
    -------
    BaseStatement
        Instance of Statement class.

    Raises
    ------
    KeyError
        Custom statement should present a `location` default parameter.
    NotImplementedError
        Declared statement type does not exist.
    """
    stmt_type = statement.get('type')

    if stmt_type == 'custom':
        location = statement.get('location')
        if not location:
            raise KeyError('A custom statement must define a `location`'
                           ' parameter.')
        cls = _load_custom_statement(location)

    elif stmt_type in STATEMENTS_MAP:
        cls = STATEMENTS_MAP[stmt_type]

    else:
        raise NotImplementedError(
            f'Statement type "{stmt_type}" not implemented.\n'
            f'The available types are {list(STATEMENTS_MAP)}'
            ' or `custom` for your own statements.'
        )

    execution_class = multibackend_class_factory(cls, backend)
    statement_instance: BaseStatement = execution_class(statement)

    return statement_instance
