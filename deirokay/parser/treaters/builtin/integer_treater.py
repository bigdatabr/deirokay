"""
Classes and functions to treat column data types according to
Deirokay data types.
"""
from typing import Iterable

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .numeric_treater import NumericTreater


class IntegerTreater(NumericTreater):
    """Treater for integer variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.INT64

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: Iterable) -> 'pandas.Series':
        return super()._treat_pandas(series).astype(float).astype('Int64')

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: Iterable
    ) -> 'dask.dataframe.Series':
        return super()._treat_dask(series).astype(float).astype('Int64')

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(
        series: 'pandas.Series'
    ) -> DeirokaySerializedSeries:
        def _convert(item):
            if item is pandas.NA:
                return None
            return int(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': IntegerTreater.supported_dtype.value
            }
        }
