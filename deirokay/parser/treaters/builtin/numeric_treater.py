"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

import dask.dataframe
import pandas

from deirokay._typing import DeirokayDataSeries
from deirokay.enums import Backend

from ..multibackend import treat
from .validator import Validator


class NumericTreater(Validator):
    """Base class for numeric treaters"""
    supported_backends = [Backend.PANDAS, Backend.DASK]

    def __init__(self, thousand_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.thousand_sep = thousand_sep

    def _treat_thousand_sep(self,
                            series: DeirokayDataSeries) -> DeirokayDataSeries:
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

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series') -> 'pandas.Series':
        series = super()._treat_pandas(series)
        series = self._treat_thousand_sep(series)

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)
        series = self._treat_thousand_sep(series)

        return series
