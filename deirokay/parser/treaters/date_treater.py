"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from pandas import NaT, Series

from .datetime64_treater import DateTime64Treater


class DateTreater(DateTime64Treater):
    """Treater for date-only variables"""

    def __init__(self, format: str = '%Y-%m-%d', **kwargs):
        super().__init__(format, **kwargs)

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        return super().treat(series).dt.date

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is None or item is NaT:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'date'
            }
        }
