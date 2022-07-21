"""
Classes and methods to load data from several sources into Deirokay.
"""

from typing import List, Type

from deirokay._typing import DeirokayReadCallable
from deirokay.backend import MultiBackendMixin
from deirokay.enums import Backend

from .dask_reader import read as read_dask
from .pandas_reader import read as read_pandas


class DataReader(MultiBackendMixin):
    """Helper class to load data from a data source in function of the
    backend."""
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]
    read: DeirokayReadCallable


DataReader.register_backend_method('read',
                                   staticmethod(read_dask),  # type: ignore  # Staticmethod is Callable # noqa: E501
                                   Backend.DASK)
DataReader.register_backend_method('read',
                                   staticmethod(read_pandas),  # type: ignore  # Staticmethod is Callable # noqa: E501
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
