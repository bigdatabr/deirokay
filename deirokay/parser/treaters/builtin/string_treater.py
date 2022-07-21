"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Iterable, Optional

import dask.dataframe  # lazy module
import numpy  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .validator import Validator


class StringTreater(Validator):
    """Treater for string variables

    Parameters
    ----------
    treat_null_as : Optional[str], optional
        Character to replace null values for, by default None
    """
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.STRING
    supported_primitives = [str]

    def __init__(self, treat_null_as: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.treat_null_as = treat_null_as

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: Iterable) -> 'pandas.Series':
        series = super()._treat_pandas(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: Iterable
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    @staticmethod
    def _serialize_common(series):
        def _convert(item):
            if item is None or item is pandas.NA or item is numpy.NaN:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': StringTreater.supported_dtype.value
            }
        }

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: 'pandas.Series') -> DeirokaySerializedSeries:
        return StringTreater._serialize_common(series)

    @serialize(Backend.DASK)
    @staticmethod
    def _serialize_dask(series: 'dask.dataframe.Series'
                        ) -> DeirokaySerializedSeries:
        return StringTreater._serialize_common(series)
