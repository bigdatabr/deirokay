"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

from pandas import Series

from .validator import Validator


class StringTreater(Validator):
    """Treater for string variables"""

    def __init__(self, treat_null_as: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.treat_null_as = treat_null_as

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is None:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'string'
            }
        }
