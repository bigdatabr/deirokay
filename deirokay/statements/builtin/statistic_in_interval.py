"""
Statement to check the number of rows in a scope.
"""
import math
from functools import partial
from typing import Any, Generator, List

import dask  # lazy module
import dask.dataframe  # lazy module
import numpy  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayStatement
from deirokay.enums import Backend

from ..multibackend import profile, report
from .base_statement import BaseStatement


def iterrec(iterable: Any) -> Generator[Any, None, None]:
    """Iterate recursively over an iterable."""
    try:
        for item in iterable:
            yield from iterrec(item)
    except TypeError:
        yield iterable


class StatisticInInterval(BaseStatement):
    """Compare the actual value of a statistic for the scope against
    a list of comparison expressions.


    The available options are:

    * `statistic`: One (or a list) of the following: 'min', 'max',
        'mean', 'std', 'var', 'count', 'nunique', 'sum', 'median',
        'mode'.
    * One or more of the following comparators:
      `<`, `<=`, `==`, `!=`, `=~`, `!~`, `>=`, `>`.
    * `atol`: Absolute tolerance (for `=~` and `!~`). Default is 0.0.
    * `rtol`: Absolute tolerance (for `=~` and `!~`). Default is 1e-09.
    * `combination_logic`: 'and' or 'or'. Default is 'and'.

    Multiple comparison expressions can be used to represent multiple
    conditions. The `combination_logic` option can be set to express
    the logical relationship when grouping two or more comparisons.

    You may provide, for instance, ['min', 'max'] as `statistic` to
    test if all values in the scope are withing a range of values.

    Examples
    --------

    To check if the mean of the 'a' column is between 0.4 and 0.6 and
    not equal (approx.) to 0.5, its standard deviation is less than 0.1
    or greater than 0.2, and all the values are between 0 and 1:

    .. code-block:: json

        {
            "scope": "a",
            "statements": [
                {
                    "name": "statistic_in_interval",
                    "statistic": "mean",
                    ">": 0.4,
                    "!~": 0.5,
                    "<": 0.6
                },
                {
                    "name": "statistic_in_interval",
                    "statistic": "std",
                    "<": 0.1,
                    ">": 0.2,
                    "combination_logic": "or"
                },
                {
                    "name": "statistic_in_interval",
                    "statistic": ["min", "max"],
                    ">=": 0,
                    "<=": 1
                },
            ]
        }

    """

    name = 'statistic_in_interval'
    expected_parameters = [
        'statistic',
        '<', '<=', '>', '>=', '==', '!=', '=~', '!~',
        'combination_logic',
        'atol', 'rtol'
    ]
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    ALLOWED_STATISTICS = ['min', 'max', 'mean', 'std', 'var', 'count',
                          'nunique', 'sum', 'median', 'mode']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.statistics = self.options['statistic']
        if not isinstance(self.statistics, list):
            self.statistics = [self.statistics]
        assert all(statistic in StatisticInInterval.ALLOWED_STATISTICS
                   for statistic in self.statistics), (
            f"Invalid statistic found."
            f" Allowed values are: {StatisticInInterval.ALLOWED_STATISTICS}"
        )
        self.combination_logic = (
            self.options.get('combination_logic', 'and').lower()
        )
        assert self.combination_logic in ('and', 'or'), (
            f"Invalid combination logic '{self.combination_logic}'."
            f" Allowed values are: 'and', 'or'."
        )
        self.less_than = self.options.get('<')
        self.less_or_equal_to = self.options.get('<=')
        self.equal_to = self.options.get('==')
        self.not_equal_to = self.options.get('!=')
        self.close_to = self.options.get('=~')
        self.not_close_to = self.options.get('!~')
        self.greater_or_equal_to = self.options.get('>=')
        self.greater_than = self.options.get('>')
        self.atol = self.options.get('atol', 0.0)
        self.rtol = self.options.get('rtol', 1e-09)

    def _calculate_statistic(self, column, statistic):
        value = getattr(column, statistic)()
        if statistic == 'mode':
            return [float(v) for v in value]
        else:
            return float(value)

    def _generate_report(self, values: List) -> dict:
        for item in values:
            if item['statistic'] == 'mode':
                item['value'] = [float(v) for v in item['value']]
            else:
                item['value'] = float(item['value'])

        if len(values) == 1:
            report_values = values[0]['value']
        else:
            report_values = values

        report = {
            'value': report_values
        }
        return report

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        values = [
            {
                'column': col,
                'statistic': statistic,
                'value': getattr(df[col], statistic)()
            }
            for col in df.columns
            for statistic in self.statistics
        ]
        return self._generate_report(values)

    @report(Backend.DASK)
    def _report_dask(self, df: 'dask.dataframe.DataFrame') -> dict:
        values = [
            {
                'column': col,
                'statistic': statistic,
                'value': getattr(df[col], statistic)()
            }
            for col in df.columns
            for statistic in self.statistics
        ]
        values, = dask.compute(values)
        return self._generate_report(values)

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        try:
            values = [item['value'] for item in report['value']]
        except (KeyError, TypeError):
            values = [report['value']]
        values = list(iterrec(values))

        is_and = (self.combination_logic == 'and')
        is_close = partial(math.isclose, abs_tol=self.atol, rel_tol=self.rtol)

        if self.less_than is not None:
            res = all(value < self.less_than for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.less_or_equal_to is not None:
            res = all(value <= self.less_or_equal_to for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.equal_to is not None:
            res = all(value == self.equal_to for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.not_equal_to is not None:
            res = all(value != self.not_equal_to for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.close_to is not None:
            res = all(is_close(value, self.close_to) for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.not_close_to is not None:
            res = all(not is_close(value, self.not_close_to) for value in values)  # noqa: E501
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.greater_or_equal_to is not None:
            res = all(value >= self.greater_or_equal_to for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        if self.greater_than is not None:
            res = all(value > self.greater_than for value in values)
            if not is_and:
                if res:
                    return True
            else:
                if not res:
                    return False

        return is_and

    @profile(Backend.PANDAS)
    @staticmethod
    def _profile_pandas(df: 'pandas.DataFrame') -> DeirokayStatement:
        if len(df.columns) > 1:
            raise NotImplementedError('Refusing to profile multiple columns')

        col = df.columns[0]

        try:
            min_value = df[col].min()
            max_value = df[col].max()

            if min_value is numpy.NaN or max_value is numpy.NaN:
                raise NotImplementedError('Cant deal with NaN')

            return {
                'type': 'statistic_in_interval',
                'statistic': ['min', 'max'],
                '>=': float(min_value),
                '<=': float(max_value)
            }
        except Exception as e:
            raise NotImplementedError('Wrong dtype for this statement') from e

    @profile(Backend.DASK)
    @staticmethod
    def _profile_dask(df: 'dask.dataframe.DataFrame') -> DeirokayStatement:
        if len(df.columns) > 1:
            raise NotImplementedError('Refusing to profile multiple columns')

        col = df.columns[0]

        try:
            min_value, max_value = dask.compute(df[col].min(), df[col].max())

            if min_value is numpy.NaN or max_value is numpy.NaN:
                raise NotImplementedError('Cant deal with NaN')

            return {
                'type': 'statistic_in_interval',
                'statistic': ['min', 'max'],
                '>=': float(min_value),
                '<=': float(max_value)
            }
        except Exception as e:
            raise NotImplementedError('Wrong dtype for this statement') from e
