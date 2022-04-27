from posixpath import splitext
from typing import Any, Dict, List, Union

import dask.dataframe


def read(data: Union[str, 'dask.dataframe.DataFrame'],
         columns: List[str],
         sql: bool = False,
         **kwargs) -> 'dask.dataframe.DataFrame':
    """Infer the file type by its extension and call the proper
    `dask.dataframe` method to parse it.

    Currently, parsing xlsx files or reading from databases is
    not supported.

    Parameters
    ----------
    data : Union[DataFrame, str]
        Path to file or SQL query, or DataFrame object
    columns : List[str]
        List of columns to be parsed.
    sql : bool, optional
        Whether or not `data` should be interpreted as a path to a file
        or a SQL query.

    Returns
    -------
    dask.dataframe.DataFrame
        The dask DataFrame.
    """
    default_kwargs: Dict[str, Any]
    if isinstance(data, dask.dataframe.DataFrame):
        return data[columns]
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
        return dask.dataframe.read_csv(data, **default_kwargs)

    elif file_extension == 'parquet':
        default_kwargs = {
            'columns': columns
        }
        default_kwargs.update(kwargs)
        return dask.dataframe.read_parquet(data, **default_kwargs)

    elif file_extension in ('xls', 'xlsx'):
        raise NotImplementedError(
            "Not able to read XLSX files as Dask dataframes"
        )
    else:
        read_ = getattr(dask.dataframe, f'read_{file_extension}', None)
        if read_ is None:
            raise TypeError(f'File type "{file_extension}" not supported')
        return read_(data, **kwargs)[columns]
