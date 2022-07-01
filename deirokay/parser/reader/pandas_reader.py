from posixpath import splitext
from typing import Any, Dict, List, Union

import pandas  # lazy module

from deirokay.fs import fs_factory


def read(data: Union[str, 'pandas.DataFrame'],
         columns: List[str],
         sql: bool = False,
         **kwargs) -> 'pandas.DataFrame':
    """Infer the file type by its extension and call the proper
    `pandas` method to parse it.

    Parameters
    ----------
    data : Union[DataFrame, dd.DataFrame, str]
        Path to file or SQL query, or DataFrame object
    columns : List[str]
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.
    **kwargs : dict
        Arguments to be passed to `pandas` methods when reading.


    Returns
    -------
    pandas.DataFrame
        The pandas DataFrame.
    """
    default_kwargs: Dict[str, Any]
    if isinstance(data, pandas.DataFrame):
        return data[columns]
    if not isinstance(data, str):
        raise TypeError(f'Unexpected type for `data` ({data.__class__})')
    if sql:
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return pandas.read_sql(data, **default_kwargs)

    file_extension = splitext(data)[1].lstrip('.')

    if file_extension == 'sql':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        query = fs_factory(data).read()
        return pandas.read_sql(query, **default_kwargs)

    elif file_extension == 'csv':
        default_kwargs = {
            'dtype': str,
            'skipinitialspace': True,
            'usecols': columns
        }
        default_kwargs.update(kwargs)
        return pandas.read_csv(data, **default_kwargs)

    elif file_extension == 'parquet':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return pandas.read_parquet(data, **default_kwargs)

    elif file_extension in ('xls', 'xlsx'):
        default_kwargs = {
            'usecols': columns
        }
        return pandas.read_excel(data, **kwargs)
    else:
        read_ = getattr(pandas, f'read_{file_extension}', None)
        if read_ is None:
            raise TypeError(f'File type "{file_extension}" not supported')
        return read_(data, **kwargs)[columns]
