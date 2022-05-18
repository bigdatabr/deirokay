"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

import dask.dataframe as dd
from pandas import NaT, Series

from .datetime64_treater import DateTime64Treater


class DateTreater(DateTime64Treater):
    """Treater for date-only variables"""

    def __init__(self, format: str = '%Y-%m-%d', **kwargs):
        super().__init__(format, **kwargs)

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        return super()._treat_pandas(series).dt.date

    # docstr-coverage:inherited
    def _treat_dask(self, series: dd.Series) -> dd.Series:
        return super()._treat_pandas(series).dt.date

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
                'dtype': 'date'
            }
        }
