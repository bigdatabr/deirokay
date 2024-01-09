"""
Classes and functions to treat column data types according to
Deirokay data types.
"""
import datetime
from typing import Any, Iterable, List

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .datetime64_treater import DateTime64Treater


class DateTreater(DateTime64Treater):
    """Treater for date-only variables"

    Parameters
    ----------
    format : str, optional
        Format to parse dates from, by default '%Y-%m-%d'
    """

    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.DATE
    supported_primitives: List[Any] = [datetime.date]

    def __init__(self, format: str = "%Y-%m-%d", **kwargs):
        super().__init__(format, **kwargs)

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: Iterable) -> "pandas.Series":
        return super()._treat_pandas(series).dt.date

    @treat(Backend.DASK)
    def _treat_dask(self, series: Iterable) -> "dask.dataframe.Series":
        return super()._treat_dask(series).dt.date

    @staticmethod
    def _serialize_common(series):
        def _convert(item):
            if item is None or item is pandas.NaT:
                return None
            return str(item)

        return {
            "values": [_convert(item) for item in series],
            "parser": {"dtype": DateTreater.supported_dtype.value},
        }

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(series: "pandas.Series") -> DeirokaySerializedSeries:
        return DateTreater._serialize_common(series)

    @serialize(Backend.DASK)
    @staticmethod
    def _serialize_dask(series: "dask.dataframe.Series") -> DeirokaySerializedSeries:
        return DateTreater._serialize_common(series)
