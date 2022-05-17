"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from pandas import NaT, Series

from .datetime64_treater import DateTime64Treater


class TimeTreater(DateTime64Treater):
    """Treater for time-only variables"""

    def __init__(self, format: str = '%H:%M:%S', **kwargs):
        super().__init__(format, **kwargs)

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        return super().treat(series).dt.time

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
                'dtype': 'time'
            }
        }
