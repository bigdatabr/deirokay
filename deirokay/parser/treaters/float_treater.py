"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

from pandas import NA, Series

from .numeric_treater import NumericTreater


class FloatTreater(NumericTreater):
    """Treater for float variables"""

    def __init__(self, decimal_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_sep = decimal_sep

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)
        series = self._treat_decimal_sep(series)

        return series.astype(float).astype('Float64')

    def _treat_decimal_sep(self, series: Series) -> Series:
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

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is NA:
                return None
            return float(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'float'
            }
        }
