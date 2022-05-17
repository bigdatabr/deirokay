"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from pandas import NaT, Series, to_datetime

from .validator import Validator


class DateTime64Treater(Validator):
    """Treater for datetime variables"""

    def __init__(self, format: str = '%Y-%m-%d %H:%M:%S', **kwargs):
        super().__init__(**kwargs)

        self.format = format

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        series = super()._treat_pandas(series)

        return to_datetime(series, format=self.format)

    # docstr-coverage:inherited
    @staticmethod
    def _serialize_pandas(series: Series) -> dict:
        def _convert(item):
            if item is None or item is NaT:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'datetime'
            }
        }
