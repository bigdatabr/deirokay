"""
Functions to parse files into pandas DataFrames.
"""
import warnings
from copy import deepcopy
from typing import Optional, Union

from deirokay.__version__ import __comp_version__
from deirokay._typing import (DeirokayColumnOptions, DeirokayDataSource,
                              DeirokayOptionsDocument)
from deirokay._utils import check_columns_in_df_columns
from deirokay.backend import detect_backend
from deirokay.enums import Backend
from deirokay.exceptions import InvalidBackend, ParsingError
from deirokay.fs import fs_factory

from .reader import reader_factory
from .treaters import get_treater_instance


def data_reader(data: Union[str, DeirokayDataSource],
                options: Union[str, DeirokayOptionsDocument],
                backend: Optional[Backend] = None,
                **kwargs) -> DeirokayDataSource:
    """Create a new tabular data from a file or an object and apply
    Deirokay treatments to correctly parse it and pre-validate its
    content.

    Parameters
    ----------
    data : Union[str, DeirokayDataSource]
        Path or object used as data source.
    options : Union[str, DeirokayOptionsDocument]
        Path to or `dict` representing a Deirokay Options document.
    backend: Optional[BackendValue], optional
        Defines backend to use for tables. By default None. Inferred
        from `data` when it is a valid DeirokayDataSource. Should be
        set when `data` is a path.

    Returns
    -------
    DeirokayDataSource
        A tabular data treated by Deirokay.
    """
    if isinstance(options, str):
        options_dict = fs_factory(options).read_dict()
    else:
        options_dict = deepcopy(options)
    options_dict.update(kwargs)
    columns = options_dict.pop('columns')

    if isinstance(data, str) and backend is None:
        if __comp_version__ < (2,):
            warnings.warn(
                'To preserve backward compatibility, the `backend` attribute'
                ' is assumed to be `Backend.PANDAS` when reading data from a'
                ' file or SQL query.\n'
                'In future, this behavior will change and an exception will'
                ' be raised whenever the backend cannot be inferred from the'
                ' `data` attribute. To prevent this error in future and'
                ' suppress this warning in the current version,'
                ' set the `backend` attribute explicitely in `data_reader()`.',
                FutureWarning
            )
            backend = Backend.PANDAS
        elif __comp_version__ >= (2,):
            raise InvalidBackend(
                'You should provide a `backend` attribute when it cannot be'
                ' inferred from `data`.'
            )
    backend = backend or detect_backend(data)

    reader = reader_factory(backend)

    df = reader.read(data, columns=list(columns), **options_dict)
    data_treater(df, columns, backend)

    return df


def data_treater(df: DeirokayDataSource,
                 options: DeirokayColumnOptions,
                 backend: Backend) -> None:
    """Receive options dict and call the proper treater class for each
    Deirokay data type.

    Parameters
    ----------
    df : DeirokayDataSource
        Raw tabular data to be treated.
    options : DeirokayColumnOptions
        Deirokay options.

    Raises
    ------
    NotImplementedError
        Data type not valid or not implemented.
    """
    check_columns_in_df_columns(options.keys(), df.columns)

    for col, opt in options.items():
        option: dict = opt.copy()

        try:
            df[col] = get_treater_instance(option, backend)(df[col])
        except Exception as e:
            raise ParsingError(f'Error when parsing "{col}".') from e
