from abc import ABC
from typing import Any, Iterable, List, Optional

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayDataSeries, DeirokaySerializedSeries
from deirokay.backend import MultiBackendMixin
from deirokay.enums import Backend, DTypes

from ..multibackend import treat as treat_


class BaseTreater(MultiBackendMixin, ABC):
    """Base class for all data treaters."""
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]
    supported_dtype: Optional[DTypes] = None
    """Optional[DTypes]: DType treated by this treater."""
    supported_primitives: List[Any] = []
    """List[Any]: List of primitives supported by this treater."""

    def __call__(self, series: DeirokayDataSeries) -> DeirokayDataSeries:
        """Proxy for `treat`."""
        return self.treat(series)

    def treat(self, series: Iterable) -> DeirokayDataSeries:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Calls the appropriate method for the detected backend

        Parameters
        ----------
        series : Iterable
            Raw series to be treated.
        """
        raise NotImplementedError

    @treat_(Backend.PANDAS, force=True)
    def _treat_pandas(self, series: Iterable) -> 'pandas.Series':
        if isinstance(series, pandas.Series):
            return series
        return pandas.Series(series)

    @treat_(Backend.DASK, force=True)
    def _treat_dask(self, series: Iterable) -> 'dask.dataframe.Series':
        if isinstance(series, dask.dataframe.Series):
            return series

        # Assumption: When treating a general Iterable, we assume
        # it fits entirely in memory
        # TODO: let user propose a max partition size,
        # instead of assuming a single partition
        return dask.dataframe.from_pandas(pandas.Series(series), npartitions=1)

    @staticmethod
    def serialize(series: DeirokayDataSeries) -> DeirokaySerializedSeries:
        """Create a Deirokay-compatible serializable object that can
        be serialized (in JSON or YAML formats) and parsed back by
        Deirokay treaters.
        This method is useful when generating validation documents
        that embed Deirokay-compatible user data.
        The output format is a Python dict containing two keys:
        - `values`: list of values as Python object.
        - `parser`: Deirokay options to parse the data.
        (See more: `deirokay.parser.get_treater_instance`).

        Parameters
        ----------
        series : DeirokayDataSeries
            Series data to be serialized.

        Returns
        -------
        dict
            A Python dict containing the keys `values` and `parser`.
        """
        raise NotImplementedError
