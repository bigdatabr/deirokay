"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

import datetime
import decimal
from decimal import Decimal
from typing import Any, Iterable, List, Optional, Type, Union

from numpy import nan
from pandas import NA, NaT, Series, Timestamp, to_datetime

from deirokay._typing import DeirokayOption
from deirokay.enums import DTypes


def get_treater_instance(option: DeirokayOption) -> 'Validator':
    """Create a treater instance from a Deirokay-style option.

    Example
    -------

    .. code-block:: python

        option = {
            'dtype': 'integer',
            'thousand_sep': ','
        }
    """
    option = option.copy()
    dtype = option.pop('dtype')

    cls = get_dtype_treater(dtype)
    return cls(**option)


def get_dtype_treater(dtype: Any) -> Type['Validator']:
    """Map a dtype to its Treater class."""

    treat_dtypes = {
        DTypes.INT64: IntegerTreater,
        DTypes.FLOAT64: FloatTreater,
        DTypes.STRING: StringTreater,
        DTypes.DATETIME: DateTime64Treater,
        DTypes.DATE: DateTreater,
        DTypes.TIME: TimeTreater,
        DTypes.BOOLEAN: BooleanTreater,
        DTypes.DECIMAL: DecimalTreater,
    }
    treat_primitives = {
        int: IntegerTreater,
        float: FloatTreater,
        str: StringTreater,
        Timestamp: DateTime64Treater,
        datetime.date: DateTreater,
        datetime.time: TimeTreater,
        bool: BooleanTreater,
        decimal.Decimal: DecimalTreater,
    }

    try:
        if isinstance(dtype, DTypes):
            return treat_dtypes[dtype]
        elif isinstance(dtype, str):
            return treat_dtypes[DTypes(dtype)]
        else:
            return treat_primitives[dtype]

    except KeyError as e:
        raise NotImplementedError(f"Handler for '{dtype}' hasn't been"
                                  " implemented yet") from e


class Validator():
    """Base validation class for column data type validation.
    """

    def __init__(self, *, unique: bool = False, nullable: bool = True):
        self.unique = unique
        self.nullable = nullable

    def __call__(self, listlike: Iterable) -> Series:
        return self.treat(Series(listlike))

    def treat(self, series: Series) -> Series:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Parameters
        ----------
        series : Series
            Raw pandas Series to be treated.

        Raises
        ------
        ValueError
            Column has null values when not_null constraint was
            requested or
            column has duplicate values when unique constraint was
            requested.
        """
        if not self.nullable and any(series.isnull()):
            null_indices = list(series[series.isnull()].index)
            null_indices_limit = null_indices[:min(len(null_indices), 30)]
            raise ValueError(f"The '{series.name}' column has"
                             f" {len(null_indices)} null values,"
                             " but it shouldn't.\n"
                             "Here are the indices of some null values:\n"
                             f"{null_indices_limit}...")

        if self.unique and not series.is_unique:
            duplicated = list(series[series.duplicated(keep='first')])
            duplicated_limit = duplicated[:min(len(duplicated), 10)]

            raise ValueError(f"The '{series.name}' column values"
                             " are not unique, as requested.\n"
                             f"There are {len(duplicated)} non unique values,"
                             " and here are some of them:\n"
                             f"{duplicated_limit}...")

        return series

    @staticmethod
    def serialize(series: Series) -> dict:
        """Create a Deirokay-compatible serializable object that can
        be serialized (in JSON or YAML formats) and parsed back by
        Deirokay treaters.
        This method is useful when generating validation documents
        that embed Deirokay-compatible user data.
        The output format is a Python dict containing two keys:
        - `values`: list of values as Python object.
        - `parser`: Deirokay options to parse the data.
        (See more: `deirokay.parser.get_treater_instance`).

        Parameters
        ----------
        series : Series
            Pandas Series to be serialized.

        Returns
        -------
        dict
            A Python dict containing the keys `values` and `parser`.
        """
        raise NotImplementedError('No serializer for this treater.')


class NumericTreater(Validator):
    """Base class for numeric treaters"""

    def __init__(self, thousand_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.thousand_sep = thousand_sep

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)
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
    def treat(self, series: Series) -> Series:
        series = super().treat(series)
        series = series.apply(self._evaluate).astype('boolean')
        # Validate again
        super().treat(series)

        return series

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
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


class FloatTreater(NumericTreater):
    """Treater for float variables"""

    def __init__(self, decimal_sep: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_sep = decimal_sep

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)
        series = self._treat_decimal_sep(series)

        return series.astype(float).astype('Float64')

    def _treat_decimal_sep(self, series: Series) -> Series:
        if self.decimal_sep is not None and self.decimal_sep != '.':
            try:
                series = series.str.replace(self.decimal_sep, '.', regex=False)
            except AttributeError as e:
                print(*e.args)
                raise AttributeError(
                    'Make sure you are not declaring a `decimal_sep` to'
                    ' read a non-text-like column. This may happen when'
                    ' reading numeric columns from a .parquet file,'
                    ' for instance.'
                )
        return series

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is NA:
                return None
            return float(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'float'
            }
        }


class DecimalTreater(FloatTreater):
    """Treater for decimal variables"""

    def __init__(self, decimal_places: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)

        self.decimal_places = decimal_places

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = NumericTreater.treat(self, series)
        series = self._treat_decimal_sep(series)
        series = series.map(lambda x: Decimal(x) if x is not None else None)
        series = self._treat_decimal_places(series)

        return series

    def _treat_decimal_places(self, series: Series) -> Series:
        if self.decimal_places is not None:
            q = Decimal(10) ** -self.decimal_places
            series = series.apply(
                lambda x: x.quantize(q) if x is not None else None
            )
        return series

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is None or item is NA:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'decimal'
            }
        }


class IntegerTreater(NumericTreater):
    """Treater for integer variables"""

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        return super().treat(series).astype(float).astype('Int64')

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is NA:
                return None
            return int(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'integer'
            }
        }


class DateTime64Treater(Validator):
    """Treater for datetime variables"""

    def __init__(self, format: str = '%Y-%m-%d %H:%M:%S', **kwargs):
        super().__init__(**kwargs)

        self.format = format

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)

        return to_datetime(series, format=self.format)

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
                'dtype': 'datetime'
            }
        }


class DateTreater(DateTime64Treater):
    """Treater for date-only variables"""

    def __init__(self, format: str = '%Y-%m-%d', **kwargs):
        super().__init__(format, **kwargs)

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        return super().treat(series).dt.date

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
                'dtype': 'date'
            }
        }


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


class StringTreater(Validator):
    """Treater for string variables"""

    def __init__(self, treat_null_as: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        self.treat_null_as = treat_null_as

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        series = super().treat(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
        def _convert(item):
            if item is None:
                return None
            return str(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'string'
            }
        }
