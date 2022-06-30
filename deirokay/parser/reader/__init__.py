"""
Classes and methods to load data from several sources into Deirokay.
"""

from typing import List, Type

from deirokay.backend import MultiBackendMixin
from deirokay.enums import Backend

from .dask_reader import read as read_dask
from .pandas_reader import read as read_pandas


class DataReader(MultiBackendMixin):
    """Helper class to load data from a data source in function of the
    backend."""
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]


DataReader.register_backend_method('read',
                                   staticmethod(read_dask),
                                   Backend.DASK)
DataReader.register_backend_method('read',
                                   staticmethod(read_pandas),
                                   Backend.PANDAS)


def reader_factory(backend: Backend) -> Type[DataReader]:
    """Return the proper reader for the given backend.

    Parameters
    ----------
    backend : Backend
        Backend.

    Returns
    -------
    Type[DataReader]
        Subclass of DataReader containing a `read` staticmethod.
    """
    return DataReader.attach_backend(backend)


__all__ = (
    'reader_factory',
)
