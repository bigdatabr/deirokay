from typing import Union

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
    }
    try:
        return _mapping[type(df)]
    except KeyError:
        raise ValueError(
            f'Unknown backend for {type(df)}.'
            f' Supported backends: {[b.value for b in _mapping.values()]}.'
        )
