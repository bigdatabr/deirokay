"""
Functions to parse files into pandas DataFrames.
"""

import datetime
from os.path import splitext
from typing import Union

import pandas as pd

from deirokay.enums import DTypes
from deirokay.fs import fs_factory
from deirokay.utils import _check_columns_in_df_columns

from . import treaters


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


def get_dtype_treater(dtype: Union[DTypes, str]) -> treaters.Validator:
    """Map a dtype to its Treater class."""
    treat_dtypes = {
        DTypes.INT64: treaters.IntegerTreater,
        int: treaters.IntegerTreater,
        DTypes.FLOAT64: treaters.FloatTreater,
        float: treaters.FloatTreater,
        DTypes.STRING: treaters.StringTreater,
        str: treaters.StringTreater,
        DTypes.DATETIME: treaters.DateTime64Treater,
        pd.Timestamp: treaters.DateTime64Treater,
        DTypes.DATE: treaters.DateTreater,
        datetime.date: treaters.DateTreater,
        DTypes.TIME: treaters.TimeTreater,
        datetime.time: treaters.TimeTreater,
        DTypes.BOOLEAN: treaters.BooleanTreater,
        bool: treaters.BooleanTreater,
    }
    if isinstance(dtype, str):
        dtype = DTypes(dtype)
    return treat_dtypes.get(dtype)


def get_treater_instance(option: dict):
    """Create a treater instance from a Deirokay-style option.

    Example
    -------

    option = {
        'dtype': 'integer',
        'thousand_sep': ','
    }
    """
    option = option.copy()
    dtype = option.pop('dtype')

    cls = get_dtype_treater(dtype)
    if not cls:
        raise NotImplementedError(f"Handler for '{dtype}' hasn't been"
                                  " implemented yet")
    return cls(**option)


def data_treater(df: pd.DataFrame, options: dict):
    """Receive options dict and call the proper treater class for each
    Deirokay data type.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame to be treated.
    options : dict
        Deirokay options.

    Raises
    ------
    NotImplementedError
        Data type not valid or not implemented.
    """
    _check_columns_in_df_columns(options.keys(), df.columns)

    for col, option in options.items():
        option: dict = option.copy()

        dtype = option.get('dtype', None)
        rename_to = option.pop('rename', None)

        if dtype is not None:
            try:
                df[col] = get_treater_instance(option)(df[col])
            except Exception as e:
                raise Exception(f'Error when parsing "{col}".') from e

        if rename_to is not None:
            df.rename(columns={col: rename_to}, inplace=True)
