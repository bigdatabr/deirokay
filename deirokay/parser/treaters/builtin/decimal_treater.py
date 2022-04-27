"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

import decimal
from decimal import Decimal
from typing import Optional

import dask.dataframe
import pandas

from deirokay._typing import DeirokayDataSeries, DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .float_treater import FloatTreater
from .numeric_treater import NumericTreater


class DecimalTreater(FloatTreater):
    """Treater for decimal variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.DECIMAL
    supported_primitives = [decimal.Decimal]

    def __init__(self, decimal_places: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_places = decimal_places

    def _treat_decimal_places(
        self, series: DeirokayDataSeries, **kwargs
    ) -> DeirokayDataSeries:
        if self.decimal_places is not None:
            q = Decimal(10) ** -self.decimal_places
            series = series.apply(
                lambda x: x.quantize(q) if x is not None else None,
                **kwargs
            )
        return series

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series') -> 'pandas.Series':
        series = NumericTreater._treat_pandas(self, series)
        series = self._treat_decimal_sep(series)
        series = series.map(lambda x: Decimal(x) if x is not None else None)
        series = self._treat_decimal_places(series)

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        series = NumericTreater._treat_dask(self, series)
        series = self._treat_decimal_sep(series, meta=(series.name, 'object'))
        series = series.map(
            lambda x: Decimal(x) if x is not None else None,
            meta=(series.name, 'object')
        )
        series = self._treat_decimal_places(series,
                                            meta=(series.name, 'object'))

        return series

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: 'pandas.Series') -> DeirokaySerializedSeries:
        def _convert(item):
            if item is None or item is pandas.NA:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': DecimalTreater.supported_dtype.value
            }
        }
