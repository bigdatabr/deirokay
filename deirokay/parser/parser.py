"""
Functions to parse files into pandas DataFrames.
"""

import datetime
import decimal
from os.path import splitext
from typing import Union

import pandas
from pandas import DataFrame, Timestamp, read_excel

from deirokay.enums import DTypes
from deirokay.fs import fs_factory
from deirokay.utils import _check_columns_in_df_columns

from . import treaters


def data_reader(data: Union[str, DataFrame],
                options: Union[dict, str]) -> DataFrame:
    """Create a DataFrame from a file or an existing DataFrame and
    apply Deirokay treatments to correctly parse it and pre-validate
    its content.

    Parameters
    ----------
    data : Union[str, DataFrame]
        [description]
    options : Union[dict, str]
        Either a `dict` or a local/S3 path to an YAML/JSON file.

    Returns
    -------
    DataFrame
        A pandas DataFrame treated by Deirokay.
    """
    if isinstance(options, str):
        options = fs_factory(options).read_dict()

    columns = options.pop('columns')

    if isinstance(data, str):
        df = pandas_read(data, columns=list(columns), **options)
    else:
        df = data.copy()[list(columns)]
    data_treater(df, columns)

    return df


def pandas_read(file_path: str, columns: list, **kwargs) -> DataFrame:
    """Infer the file type by its extension and call the proper
    `pandas` method to parse it.

    Parameters
    ----------
    file_path : str
        Path to file.
    columns : list
        List of columns to be parsed.

    Returns
    -------
    DataFrame
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
            'usecols': columns
        },
        'parquet': {
            'columns': columns
        }
    }
    pandas_kwargs.update(default_args_by_extension.get(file_extension, {}))
    pandas_kwargs.update(kwargs)

    pd_read_func = getattr(pandas, f'read_{file_extension}', None)
    if pd_read_func is None:
        other_readers = {
            'xls': read_excel,
            'xlsx': read_excel
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
        Timestamp: treaters.DateTime64Treater,
        DTypes.DATE: treaters.DateTreater,
        datetime.date: treaters.DateTreater,
        DTypes.TIME: treaters.TimeTreater,
        datetime.time: treaters.TimeTreater,
        DTypes.BOOLEAN: treaters.BooleanTreater,
        bool: treaters.BooleanTreater,
        DTypes.DECIMAL: treaters.DecimalTreater,
        decimal.Decimal: treaters.DecimalTreater,
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


def data_treater(df: DataFrame, options: dict):
    """Receive options dict and call the proper treater class for each
    Deirokay data type.

    Parameters
    ----------
    df : DataFrame
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
