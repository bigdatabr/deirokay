"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

import dask.dataframe
import pandas

from deirokay._typing import DeirokayDataSeries, DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .numeric_treater import NumericTreater


class FloatTreater(NumericTreater):
    """Treater for float variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.FLOAT64
    supported_primitives = [float]

    def __init__(self, decimal_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_sep = decimal_sep

    def _treat_decimal_sep(
        self, series: DeirokayDataSeries, **kwargs
    ) -> DeirokayDataSeries:
        if self.decimal_sep is not None and self.decimal_sep != '.':
            try:
                series = series.str.replace(self.decimal_sep, '.', regex=False)
            except AttributeError as e:
                print(*e.args)
                raise AttributeError(
                    'Make sure you are not declaring a `decimal_sep` to'
                    ' read a non-text-like column. This may happen when'
                    ' reading numeric columns from a .parquet file,'
                    ' for instance.'
                )
        return series

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series') -> 'pandas.Series':
        series = super()._treat_pandas(series)
        series = self._treat_decimal_sep(series)

        return series.astype(float).astype('Float64')

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)
        series = self._treat_decimal_sep(series)

        return series.astype(float).astype('Float64')

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: 'pandas.Series') -> DeirokaySerializedSeries:
        def _convert(item):
            if item is pandas.NA:
                return None
            return float(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': FloatTreater.supported_dtype.value
            }
        }
