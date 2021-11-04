"""
Functions to parse files into pandas DataFrames.
"""

from os.path import splitext
from typing import Union

import pandas as pd

from ..enums import DTypes
from ..fs import fs_factory
from .treaters import data_treater


def data_reader(data: Union[str, pd.DataFrame],
                options: Union[dict, str]) -> pd.DataFrame:
    """Create a DataFrame from a file or an existing DataFrame and
    apply Deirokay treatments to correctly parse it and pre-validate
    its content.

    Parameters
    ----------
    data : Union[str, pd.DataFrame]
        [description]
    options : Union[dict, str]
        Either a `dict` or a local/S3 path to an YAML/JSON file.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame treated by Deirokay.
    """
    if isinstance(options, str):
        options = fs_factory(options).read_dict()

    for column in options.get('columns').values():
        column['dtype'] = DTypes(column['dtype'])

    columns = options.pop('columns')

    if isinstance(data, str):
        df = pandas_read(data, **options)
    else:
        df = data.copy()
    data_treater(df, columns)

    return df


def pandas_read(file_path: str, **kwargs) -> pd.DataFrame:
    """Infer the file type by its extension and call the proper
    `pandas` method to parse it.

    Parameters
    ----------
    file_path : str
        Path to file.

    Returns
    -------
    pd.DataFrame
        The pandas DataFrame.

    Raises
    ------
    TypeError
        File extension/type not supported.
    """
    file_extension = splitext(file_path)[1].lstrip('.')

    pandas_kwargs = {}
    default_args_by_extension = {
        'csv': {
            'dtype': str,
            'skipinitialspace': True,
        }
    }
    pandas_kwargs.update(default_args_by_extension.get(file_extension, {}))
    pandas_kwargs.update(kwargs)

    pd_read_func = getattr(pd, f'read_{file_extension}', None)
    if pd_read_func is None:
        other_readers = {
            'xls': pd.read_excel,
            'xlsx': pd.read_excel
        }
        try:
            pd_read_func = other_readers[file_extension]
        except KeyError:
            raise TypeError('File type not supported')

    return pd_read_func(file_path, **pandas_kwargs)
