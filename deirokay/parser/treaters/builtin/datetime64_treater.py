"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

import datetime
from typing import Iterable

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .validator import Validator

_supported_primitives = [datetime.datetime]
try:
    _supported_primitives.append(pandas.Timestamp)
except ModuleNotFoundError:
    pass


class DateTime64Treater(Validator):
    """Treater for datetime variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.DATETIME
    supported_primitives = _supported_primitives

    def __init__(self, format: str = '%Y-%m-%d %H:%M:%S', **kwargs):
        super().__init__(**kwargs)

        self.format = format

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: Iterable) -> 'pandas.Series':
        series = super()._treat_pandas(series)

        return pandas.to_datetime(series, format=self.format)

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: Iterable
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)

        return dask.dataframe.to_datetime(series, format=self.format)

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(
        series: 'pandas.Series'
    ) -> DeirokaySerializedSeries:
        def _convert(item):
            if item is None or item is pandas.NaT:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': DateTime64Treater.supported_dtype.value
            }
        }
