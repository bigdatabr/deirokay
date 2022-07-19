"""
Statement to check the number of rows in a scope.
"""
from typing import List

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayStatement
from deirokay.enums import Backend

from ..multibackend import profile, report
from .base_statement import BaseStatement


class RowCount(BaseStatement):
    """Check if the number of rows (or the number of of distinct rows)
    in a scope is between a minimum and maximum value.

    The available options are:

    * `min`: The minimum number of rows. If None, no minimum is
      enforced. Default: None.
    * `max`: The maximum number of rows. If None, no maximum is
      enforced. Default: None.
    * `distinct`: If True, check the number of distinct rows instead of
      the total number of rows. Default: False.

    Providing no `min` or `max` parameters, the statement will act only
    as a logger for its statistics.

    When counting the total number of rows (`distinct=False`), this
    statement may be applied to any scope of your DataFrame, since
    every column would have the same number of rows.
    By convention, you should apply it to a scope containing all the
    columns of your DataFrame.

    To count the number of (not-)null rows, you should use the `not_null`
    statement instead.
    To count the number of unique rows, use the `unique` statement.

    Examples
    --------
    * After some historial analysis of your data, you found that the
      number of rows is always greater or equal to than 42.
      You can declare the following validation item to represent this
      rule:

    .. code-block:: json

        {
            "scope": ["foo", "bar"],
            "statements": [
                {
                    "name": "row_count",
                    "min": 42
                }
            ]
        }

    * You have a table of daily transactions from all branches of a
      company. Not all branches have transactions for every day, and
      new branches may be added at any time. You want to ensure that
      the number of branches that appears in your data does not vary
      sharply downwards (below 5% of its 7-day historical average),
      which could be a sign of failure to receive transactions from
      some branches.
      You can declare the following validation item (in YAML format) to
      check this rule:

    .. code-block:: yaml

        scope: branch_name
        statements:
        - name: row_count
          distinct: True
          min: >
            {{ 0.95 * (
              series("transactions", 7).branch_name.row_count.distinct_rows.mean()  # noqa E501
              | default(19, true))
              | float
            ) }}

    There are many things going on here:

    * In YAML, the ">" operator is used to collapse a multi-line string
      into a single line. In JSON you would have to put everything in
      the same line;
    * The "{{}}" braces are used to indicate that the following
      expression is a Jinja2 template.
    * The `series` function is a built-in Deirokay method
      used to get the 7-day historical validations.
      Further down, the `mean` function is used to
      compute the 7-day average of the `distinct_rows` metric returned
      by the `row_count` statement in the `branch_name` scope.
    * The "|" operator inside the Jinja2 template is used to apply
      a function to the result of the previous expression, such that in
      the end we obtain a float.
    * The `default` function is used to set a default value if the
      previous expression is `None`.
    * The `float` function is used to convert the result of the
      previous expression, which is a `numpy.float64`, to a float,
      which can be properly serialized in JSON or YAML format when the
      validation logs are generated.

    For this example to work, you will need to declare in your 
    `deirokay.validate` call the `save_to` parameters, so that the
    validation logs can be saved and later used to provide historical
    analysis.

    .. code-block:: python

        from deirokay import validate

        validate(df, against=assertions, save_to='logs')

    """

    name = 'row_count'
    expected_parameters = ['min', 'max', 'distinct']
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.min = self.options.get('min', None)
        self.max = self.options.get('max', None)
        self.distinct = self.options.get('distinct', False)

    def _report_common(self, df):
        row_count = len(df)
        distinct_count = len(df.drop_duplicates())

        return {
            'rows': row_count,
            'distinct_rows': distinct_count,
        }

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        return self._report_common(df)

    @report(Backend.DASK)
    def _report_dask(self, df: 'dask.dataframe.DataFrame') -> dict:
        return self._report_common(df)

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        if self.distinct:
            count = report['distinct_rows']
        else:
            count = report['rows']

        if self.min is not None:
            if not count >= self.min:
                return False
        if self.max is not None:
            if not count <= self.max:
                return False
        return True

    @staticmethod
    def _profile_common(df):
        statement: DeirokayStatement

        if len(df.columns) > 1:
            count = len(df)
            statement = {
                'type': 'row_count',
                'min': count,
                'max': count
            }
        else:
            count = len(df.drop_duplicates())
            statement = {
                'type': 'row_count',
                'distinct': True,
                'min': count,
                'max': count
            }
        return statement

    @profile(Backend.PANDAS)
    @staticmethod
    def _profile_pandas(df: 'pandas.DataFrame') -> DeirokayStatement:
        return RowCount._profile_common(df)

    @profile(Backend.DASK)
    @staticmethod
    def _profile_dask(df: 'dask.dataframe.DataFrame') -> DeirokayStatement:
        return RowCount._profile_common(df)
