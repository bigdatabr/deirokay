"""
Functions to parse files into pandas DataFrames.
"""

import datetime
import decimal
from os.path import splitext
from typing import Union

import pandas
from pandas import (DataFrame, Timestamp, read_csv, read_excel, read_parquet,
                    read_sql)

from deirokay.enums import DTypes
from deirokay.fs import fs_factory
from deirokay.utils import _check_columns_in_df_columns

from . import treaters


def data_reader(data: Union[str, DataFrame],
                options: Union[dict, str],
                **kwargs) -> DataFrame:
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
    options.update(kwargs)

    columns = options.pop('columns')

    if isinstance(data, str):
        df = pandas_read(data, columns=list(columns), **options)
    else:
        df = data.copy()[list(columns)]
    data_treater(df, columns)

    return df


def pandas_read(data: str, columns: list, sql: bool = False,
                **kwargs) -> DataFrame:
    """Infer the file type by its extension and call the proper
    `pandas` method to parse it.

    Parameters
    ----------
    data : str
        Path to file or SQL query.
    columns : list
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.


    Returns
    -------
    DataFrame
        The pandas DataFrame.
    """
    if sql:
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return read_sql(data, **default_kwargs)

    file_extension = splitext(data)[1].lstrip('.')

    if file_extension == 'sql':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        query = fs_factory(data).read()
        return read_sql(query, **default_kwargs)

    elif file_extension == 'csv':
        default_kwargs = {
            'dtype': str,
            'skipinitialspace': True,
            'usecols': columns
        }
        default_kwargs.update(kwargs)
        return read_csv(data, **default_kwargs)

    elif file_extension == 'parquet':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return read_parquet(data, **default_kwargs)

    elif file_extension in ('xls', 'xlsx'):
        default_kwargs = {
            'usecols': columns
        }
        return read_excel(data, **kwargs)
    else:
        read_ = getattr(pandas, f'read_{file_extension}', None)
        if read_ is None:
            raise TypeError(f'File type "{file_extension}" not supported')
        return read_(data, **kwargs)[columns]


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

    .. code-block:: python

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
