"""
Functions to parse files into pandas DataFrames.
"""

import datetime
import decimal
from os.path import splitext
from typing import Any, Dict, List, Type, Union

import dask.dataframe as dd
import pandas
from pandas import (DataFrame, Timestamp, read_csv, read_excel, read_parquet,
                    read_sql)

from deirokay._typing import DeirokayOption, DeirokayOptionsDocument
from deirokay.enums import DTypes
from deirokay.fs import fs_factory
from deirokay.utils import _check_columns_in_df_columns

from . import treaters


def data_reader(data: Union[str, DataFrame],
                options: Union[str, DeirokayOptionsDocument],
                dask=False,
                **kwargs) -> Union[DataFrame, dd.DataFrame]:
    """Create a DataFrame from a file or an existing DataFrame and
    apply Deirokay treatments to correctly parse it and pre-validate
    its content.

    Parameters
    ----------
    data : Union[str, DataFrame]
        [description]
    options : Union[dict, str]
        Either a `dict` or a local/S3 path to an YAML/JSON file.
    dask: bool, default False
        Whether to return a pandas or dask DataFrame.

    Returns
    -------
    Union[dd.DataFrame, DataFrame]
        A pandas or dask DataFrame treated by Deirokay.
    """
    if isinstance(options, str):
        options_dict = fs_factory(options).read_dict()
    else:
        options_dict = options

    options_dict.update(kwargs)

    columns = options_dict.pop('columns')

    _reader = dask_read if dask else pandas_read
    if isinstance(data, str):
        df = _reader(data, columns=list(columns), **options_dict)
    else:
        df = data.copy()[list(columns)]
        if dask:
            df = dd.from_pandas(df, npartitions=1)
    data_treater(df, columns)

    return df


def pandas_read(data: str, columns: List[str], sql: bool = False,
                **kwargs) -> DataFrame:
    """Infer the file type by its extension and call the proper
    `pandas` method to parse it.

    Parameters
    ----------
    data : str
        Path to file or SQL query.
    columns : List[str]
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.


    Returns
    -------
    DataFrame
        The pandas DataFrame.
    """
    default_kwargs: Dict[str, Any]
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


def dask_read(data: str, columns: List[str], sql: bool = False,
              **kwargs) -> dd.DataFrame:
    """Infer the file type by its extension and call the proper
    `dask` method to parse it.

    Currently, parsing xlsx files or reading from databases is
    not supported.

    Parameters
    ----------
    data : str
        Path to file or SQL query.
    columns : List[str]
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.


    Returns
    -------
    DataFrame
        The pandas DataFrame.
    """
    default_kwargs: Dict[str, Any]
    if sql:
        raise NotImplementedError(
            "Reading SQL queries into Dask dataframes is not supported."
        )

    file_extension = splitext(data)[1].lstrip('.')

    if file_extension == 'sql':
        raise NotImplementedError(
            "Reading SQL queries into Dask dataframes is not supported."
        )

    elif file_extension == 'csv':
        default_kwargs = {
            'dtype': str,
            'skipinitialspace': True,
            'usecols': columns
        }
        default_kwargs.update(kwargs)
        return dd.read_csv(data, **default_kwargs)

    elif file_extension == 'parquet':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return dd.read_parquet(data, **default_kwargs)

    elif file_extension in ('xls', 'xlsx'):
        raise NotImplementedError(
            "Not able to read XLSX files as Dask dataframes"
        )
    else:
        read_ = getattr(dd, f'read_{file_extension}', None)
        if read_ is None:
            raise TypeError(f'File type "{file_extension}" not supported')
        return read_(data, **kwargs)[columns]


def get_dtype_treater(dtype: Any) -> Type[treaters.Validator]:
    """Map a dtype to its Treater class."""

    treat_dtypes = {
        DTypes.INT64: treaters.IntegerTreater,
        DTypes.FLOAT64: treaters.FloatTreater,
        DTypes.STRING: treaters.StringTreater,
        DTypes.DATETIME: treaters.DateTime64Treater,
        DTypes.DATE: treaters.DateTreater,
        DTypes.TIME: treaters.TimeTreater,
        DTypes.BOOLEAN: treaters.BooleanTreater,
        DTypes.DECIMAL: treaters.DecimalTreater,
    }
    treat_primitives = {
        int: treaters.IntegerTreater,
        float: treaters.FloatTreater,
        str: treaters.StringTreater,
        Timestamp: treaters.DateTime64Treater,
        datetime.date: treaters.DateTreater,
        datetime.time: treaters.TimeTreater,
        bool: treaters.BooleanTreater,
        decimal.Decimal: treaters.DecimalTreater,
    }

    try:
        if isinstance(dtype, DTypes):
            return treat_dtypes[dtype]
        elif isinstance(dtype, str):
            return treat_dtypes[DTypes(dtype)]
        else:
            return treat_primitives[dtype]

    except KeyError as e:
        raise NotImplementedError(f"Handler for '{dtype}' hasn't been"
                                  " implemented yet") from e


def get_treater_instance(option: DeirokayOption) -> treaters.Validator:
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
    return cls(**option)


def data_treater(df: Union[DataFrame, dd.DataFrame], options: dict) -> None:
    """Receive options dict and call the proper treater class for each
    Deirokay data type.

    Parameters
    ----------
    df : Union[DataFrame, dd.DataFrame]
        Raw DataFrame to be treated.
    options : dict
        Deirokay options.

    Raises
    ------
    NotImplementedError
        Data type not valid or not implemented.
    """
    _check_columns_in_df_columns(options.keys(), df.columns)

    for col, opt in options.items():
        option: dict = opt.copy()

        dtype = option.get('dtype', None)
        rename_to = option.pop('rename', None)

        if dtype is not None:
            try:
                df[col] = get_treater_instance(option)(df[col])
            except Exception as e:
                raise Exception(f'Error when parsing "{col}".') from e

        if rename_to is not None:
            df.rename(columns={col: rename_to}, inplace=True)
