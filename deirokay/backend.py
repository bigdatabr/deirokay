from typing import Union

import dask.dataframe as dd
import pandas as pd
from dask.dataframe import DataFrame as DaskDataFrame
from pandas import DataFrame

from deirokay.enums import Backend


def detect_backend(df: Union[DataFrame, DaskDataFrame]) -> Backend:
    """Map the object class to the proper backend value.

    Parameters
    ----------
    df : Union[DataFrame, DaskDataFrame]
        DataFrame object.

    Returns
    -------
    Backend
        Instance of `Backend` enumeration.

    Raises
    ------
    ValueError
        Unknown backend for the specified object type.
    """

    _mapping = {
        DataFrame: Backend.PANDAS,
        DaskDataFrame: Backend.DASK,
        pd.Series: Backend.PANDAS,
        dd.Series: Backend.DASK
    }
    try:
        return _mapping[type(df)]
    except KeyError:
        raise ValueError(
            f'Unknown backend for {type(df)}. '
            f'Supported backends: {set([b.value for b in _mapping.values()])}.'
        )


def backend_from_str(backend: str) -> Backend:
    """Map a backend name to the proper backend value.

    Parameters
    ----------
    backend: str
        Backend name

    Returns
    -------
    Backend
        Instance of `Backend` enum.

    Raises
    ------
    ValueError
        Unknown backend for the specified object type

    """
    _mapping = {
        'pandas': Backend.PANDAS,
        'dask': Backend.DASK
    }
    try:
        return _mapping[backend.strip().lower()]
    except KeyError:
        raise ValueError(
            f'No backend {backend}.\n'
            f'Select one of {", ".join(_mapping.keys())}'
        )
