"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Optional

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .validator import Validator


class StringTreater(Validator):
    """Treater for string variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.STRING
    supported_primitives = [str]

    def __init__(self, treat_null_as: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.treat_null_as = treat_null_as

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series') -> 'pandas.Series':
        series = super()._treat_pandas(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: 'pandas.Series') -> DeirokaySerializedSeries:
        def _convert(item):
            if item is None:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': StringTreater.supported_dtype.value
            }
        }
