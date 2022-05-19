"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from decimal import Decimal
from typing import Optional

import dask.dataframe as dd
from pandas import NA, Series

from .float_treater import FloatTreater
from .numeric_treater import NumericTreater


class DecimalTreater(FloatTreater):
    """Treater for decimal variables"""

    def __init__(self, decimal_places: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_places = decimal_places

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        series = NumericTreater._treat_pandas(self, series)
        series = self._treat_decimal_sep(series)
        series = series.map(lambda x: Decimal(x) if x is not None else None)
        series = self._treat_decimal_places(series)

        return series

    # docstr-coverage:inherited
    def _treat_dask(self, series: dd.Series) -> dd.Series:
        series = NumericTreater._treat_dask(self, series)
        series = self._treat_decimal_sep(series, meta=(series.name, 'object'))
        series = series.map(
            lambda x: Decimal(x) if x is not None else None,
            meta=(series.name, 'object')
        )
        series = self._treat_decimal_places(series,
                                            meta=(series.name, 'object'))

        return series

    def _treat_decimal_places(self, series: Series, **kwargs) -> Series:
        if self.decimal_places is not None:
            q = Decimal(10) ** -self.decimal_places
            series = series.apply(
                lambda x: x.quantize(q) if x is not None else None,
                **kwargs
            )
        return series

    # docstr-coverage:inherited
    @staticmethod
    def _serialize_pandas(series: Series) -> dict:
        def _convert(item):
            if item is None or item is NA:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'decimal'
            }
        }
