"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Iterable, List, Optional, Union

import dask.dataframe  # lazy module
import numpy  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokaySerializedSeries
from deirokay.enums import Backend, DTypes

from ..multibackend import serialize, treat
from .validator import Validator


class BooleanTreater(Validator):
    """Treater for boolean-like variables"""
    supported_backends = [Backend.PANDAS, Backend.DASK]
    supported_dtype = DTypes.BOOLEAN
    supported_primitives = [bool]

    def __init__(self,
                 truthies: List[str] = ['true', 'True'],
                 falsies: List[str] = ['false', 'False'],
                 ignore_case: bool = False,
                 default_value: Optional[bool] = None,
                 **kwargs):
        super().__init__(**kwargs)

        assert default_value in (True, False, None)

        self.ignore_case = ignore_case
        self.default_value = default_value

        if self.ignore_case:
            self.truthies = set(truthy.lower() for truthy in truthies)
            self.falsies = set(falsy.lower() for falsy in falsies)
        else:
            self.truthies = set(truthies)
            self.falsies = set(falsies)

        if self.truthies & self.falsies:
            raise ValueError('Truthies and Falsies sets should be'
                             ' disjoint sets.')

    def _evaluate(self, value: Union[bool, str, None]) -> Union[bool, None]:
        if value is True:
            return True
        if value is False:
            return False
        if value is None or value is numpy.nan or value is pandas.NA:
            return self.default_value

        _value = value.lower() if self.ignore_case else value
        if _value in self.truthies:
            return True
        if _value in self.falsies:
            return False
        raise ValueError(f'Unexpected boolean value: "{value}"'
                         f' ({value.__class__})\n'
                         f'Expected values: {self.truthies | self.falsies}')

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: Iterable) -> 'pandas.Series':
        series = super()._treat_pandas(series)
        series = series.apply(self._evaluate).astype('boolean')
        # Validate again
        super()._treat_pandas(series)

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: Iterable
    ) -> 'dask.dataframe.Series':
        series = super()._treat_dask(series)
        series = series.apply(
            self._evaluate, meta=(series.name, 'object')
        ).astype('boolean')
        # Validate again
        super()._treat_dask(series)

    @staticmethod
    def _serialize_common(series):
        def _convert(item):
            if item is pandas.NA:
                return None
            return bool(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': BooleanTreater.supported_dtype.value
            }
        }

    @serialize(Backend.PANDAS)
    @staticmethod
    def _serialize_pandas(
        series: 'pandas.Series'
    ) -> DeirokaySerializedSeries:
        return BooleanTreater._serialize_common(series)

    @serialize(Backend.DASK)
    @staticmethod
    def _serialize_dask(
        series: 'dask.dataframe.Series'
    ) -> DeirokaySerializedSeries:
        return BooleanTreater._serialize_common(series)
