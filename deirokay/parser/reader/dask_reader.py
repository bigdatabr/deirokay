from posixpath import splitext
from typing import Any, Dict, List, Union

import dask.dataframe  # lazy module


def read(data: Union[str, 'dask.dataframe.DataFrame'],
         columns: List[str],
         sql: bool = False,
         **kwargs) -> 'dask.dataframe.DataFrame':
    """Infer the file type by its extension and call the proper
    `dask.dataframe` method to parse it.

    Parameters
    ----------
    data : Union[str, dask.dataframe.DataFrame]
        Path to file or SQL query, or DataFrame object
    columns : List[str]
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.
    **kwargs : dict
        Arguments to be passed to `dask.dataframe` methods when reading.

    Returns
    -------
    dask.dataframe.DataFrame
        The dask DataFrame.
    """
    default_kwargs: Dict[str, Any] = {}

    if isinstance(data, dask.dataframe.DataFrame):
        return data[columns]

    if not isinstance(data, str):
        raise TypeError(f'Unexpected type for `data` ({data.__class__})')

    if sql:
        read_ = dask.dataframe.read_sql

    else:
        file_extension = splitext(data)[1].lstrip('.').lower()

        if file_extension == 'csv':
            default_kwargs.update({
                'dtype': str,
                'skipinitialspace': True,
            })

        read_ = getattr(dask.dataframe, f'read_{file_extension}', None)
        if read_ is None:
            raise TypeError(f'File type "{file_extension}" not supported')

    default_kwargs.update(kwargs)

    # try `columns` argument
    try:
        return read_(data, columns=columns, **default_kwargs)
    except TypeError as e:
        if 'columns' not in str(e):
            raise e
    # try `usecols` argument
    try:
        return read_(data, usecols=columns, **default_kwargs)
    except TypeError as e:
        if 'usecols' not in str(e):
            raise e
    # give up, read everything, filter columns later
    return read_(data, **default_kwargs)[columns]
