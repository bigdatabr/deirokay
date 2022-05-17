"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import List, Optional, Union

from numpy import nan
from pandas import NA, Series

from .validator import Validator


class BooleanTreater(Validator):
    """Treater for boolean-like variables"""

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
        if value is None or value is nan or value is NA:
            return self.default_value

        _value = value.lower() if self.ignore_case else value
        if _value in self.truthies:
            return True
        if _value in self.falsies:
            return False
        raise ValueError(f'Unexpected boolean value: "{value}"'
                         f' ({value.__class__})\n'
                         f'Expected values: {self.truthies | self.falsies}')

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        series = super()._treat_pandas(series)
        series = series.apply(self._evaluate).astype('boolean')
        # Validate again
        super()._treat_pandas(series)

        return series

    # docstr-coverage:inherited
    @staticmethod
    def _serialize_pandas(series: Series) -> dict:
        def _convert(item):
            if item is NA:
                return None
            return bool(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'boolean'
            }
        }
