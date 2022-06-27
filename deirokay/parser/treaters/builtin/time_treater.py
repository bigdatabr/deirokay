"""
Classes and functions to treat column data types according to
Deirokay data types.
"""
import datetime

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .datetime64_treater import DateTime64Treater


class TimeTreater(DateTime64Treater):
    """Treater for time-only variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.TIME
    supported_primitives = [datetime.time]

    def __init__(self, format: str = '%H:%M:%S', **kwargs):
        super().__init__(format, **kwargs)

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series') -> 'pandas.Series':
        return super()._treat_pandas(series).dt.time

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        return super()._treat_dask(series).dt.time

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: 'pandas.Series') -> DeirokaySerializedSeries:
        def _convert(item):
            if item is None or item is pandas.NaT:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': TimeTreater.supported_dtype.value
            }
        }
