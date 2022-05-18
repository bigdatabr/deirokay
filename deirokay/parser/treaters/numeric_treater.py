"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

import dask.dataframe as dd
from pandas import Series

from .validator import Validator


class NumericTreater(Validator):
    """Base class for numeric treaters"""

    def __init__(self, thousand_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.thousand_sep = thousand_sep

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        series = super()._treat_pandas(series)
        series = self._treat_thousand_sep(series)

        return series

    # docstr-coverage:inherited
    def _treat_dask(self, series: dd.Series) -> dd.Series:
        series = super()._treat_dask(series)
        series = self._treat_thousand_sep(series)

        return series

    def _treat_thousand_sep(self, series: Series) -> Series:
        if self.thousand_sep is not None:
            try:
                series = series.str.replace(self.thousand_sep, '', regex=False)
            except AttributeError as e:
                raise AttributeError(
                    'Make sure you are not declaring a `thousand_sep` to'
                    ' read a non-text-like column. This may happen when'
                    ' reading numeric columns from a .parquet file,'
                    ' for instance.'
                ) from e
        return series
