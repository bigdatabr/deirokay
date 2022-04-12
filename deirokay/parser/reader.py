"""
Functions to parse files into pandas DataFrames.
"""

from os.path import splitext
from typing import Any, Dict, List, Union

import pandas
from pandas import DataFrame, read_csv, read_excel, read_parquet, read_sql

from deirokay._typing import DeirokayOptionsDocument
from deirokay.core import DeirokayDataFrame
from deirokay.fs import fs_factory


def data_reader(data: Union[str, DataFrame],
                options: Union[str, DeirokayOptionsDocument],
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
        options_doc = fs_factory(options).read_dict()
    else:
        options_doc = options

    options_doc.update(kwargs)

    other_opts = options_doc.copy()
    opt_cols = other_opts.pop('columns')

    if isinstance(data, str):
        df = pandas_read(data, columns=list(opt_cols), **other_opts)
    elif isinstance(data, DataFrame):
        df = data[list(opt_cols)]
    else:
        raise TypeError(
            f"data must be a str or a DataFrame, not {type(data)}"
        )
    df = dataframe_treater(df, options_doc)

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


def dataframe_treater(df: DataFrame,
                      options_doc: DeirokayOptionsDocument) -> None:
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

    df = DeirokayDataFrame(df, options_doc=options_doc)
    df.treat()
    return df
