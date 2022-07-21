"""
Statement to evaluate mathematical expressions againt a set of
columns from a given scope.
"""
import re
from decimal import Decimal
from functools import reduce
from typing import List, Tuple

import dask  # lazy module
import dask.dataframe  # lazy module
import numpy  # lazy module
import pandas  # lazy module

from deirokay.enums import Backend

from ..multibackend import report
from .base_statement import BaseStatement


class ColumnExpression(BaseStatement):
    """
    Evaluates an expression (or a list of expressions) involving
    the scope columns, using `numpy.eval()`.
    The statement passes only if all expressions evaluate to `True`.

    The columns in the scope must be ideally of the same dtype.
    This statement supports the following dtypes:
    `string`, `integer`, `float` and `decimal`.

    The available parameters for this statement are:

    * `expressions` (required): an expression (or a `list` of
      expressions) to be evaluated.
      The valid operators are: `==`, `!=`, `=~`, `>=`, `<=`, `>` and
      `<`.
    * `at_least_%`: the minimum percentage of valid rows. Default: 100.
    * `at_most_%`: the maximum percentage of valid rows. Default: 100.
    * `rtol`: the relative tolerance for float evaluations (when
      using the `=~` operator). Default: 1e-5.
    * `atol`: the absolute tolerance for float evaluations (when
      using the `=~` operator). Default: 1e-8.

    Examples
    --------
    In the example below, in JSON format, we test whether or not the
    values of the `a` column are equal to the values of the `b` column.
    Similarly, we test whether or not the values of the `b` column are
    greater than the values of the `c` column:

    .. code-block:: json

        {
            "scope": ["a", "b", "c"],
            "statements": [
                {
                    "type": "column_expression",
                    "expressions": ["a == b", "a < c"],
                    "at_least_%": 50.0
                }
            ]
        }

    For float comparisons, you may prefer using the `rtol` or `atol`
    parameters, in addition to the `=~` operator. For example, if you
    want to test whether or not the values of the `a` column are equal
    to the values of the `b` column with a relative tolerance of 1e-3,
    you can use the following JSON:

    .. code-block:: json

        {
            "scope": ["a", "b"],
            "statements": [
                {
                    "type": "column_expression",
                    "expressions": "a =~ b",
                    "rtol": 1e-3
                }
            ]
        }

    """

    name = 'column_expression'
    expected_parameters = [
        'expressions',
        'at_least_%',
        'at_most_%',
        'rtol',
        'atol'
    ]
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    VALID_OPERATORS = '|'.join(['==', '!=', '=~', '>=', '<=', '>', '<'])

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.expressions = self.options['expressions']
        self.at_least_perc = self.options.get('at_least_%', 100.0)
        self.at_most_perc = self.options.get('at_most_%', 100.0)
        self.rtol = self.options.get('rtol', 1e-5)
        self.atol = self.options.get('atol', 1e-8)

        if not isinstance(self.expressions, list):
            self.expressions = [self.expressions]

    def _convert_df_dtypes(self, df: 'pandas.DataFrame') -> 'pandas.DataFrame':
        """
        Fixes DataFrame dtypes. If Int64Dtype() or Float64Dtype(),
        converts to traditional int64 and float64 dtypes. If object
        dtype, we apply a more accurate check on the column type,
        verifying the dtype of all its cells.

        When a pandas version corrects this bug, we can delete this
        method.
        """
        pandas_dtypes_int = [pandas.Int64Dtype(), pandas.Int32Dtype()]
        pandas_dtypes_float = [pandas.Float64Dtype(), pandas.Float32Dtype()]
        pandas_dtypes_decimal = [Decimal]

        def _fix_column(column: pandas.Series):
            if column.dtype in pandas_dtypes_int:
                column = column.astype(int)
            elif column.dtype in pandas_dtypes_float:
                column = column.astype(float)
            elif column.dtype == object:
                if len(column.map(type).drop_duplicates()) > 1:
                    raise Exception('Mixed types')
                if type(column[0]) in pandas_dtypes_decimal:
                    column = column.astype(float)
            return column

        return df.apply(_fix_column)

    def _eval(self, df: 'pandas.DataFrame', expr: str) -> 'pandas.Series':
        """
        Accomplishes the paper of `pandas.eval` when we have the
        `=~` comparison to evaluate. That implementation is done by
        using `numpy.eval`.
        """
        if '=~' not in expr:
            return df.eval(expr)

        expr_terms = re.split(ColumnExpression.VALID_OPERATORS, expr)
        expr_operators = re.findall(ColumnExpression.VALID_OPERATORS, expr)

        if len(expr_terms) != len(expr_operators) + 1:
            raise SyntaxError(
                'Invalid expression. Incoherent number of expressions and'
                ' comparison operators'
            )

        def _eval_part(term_1, operator, term_2):
            if operator == '=~':
                return pandas.Series(
                    numpy.isclose(
                        df.eval(term_1),
                        df.eval(term_2),
                        atol=self.atol,
                        rtol=self.rtol
                    )
                )
            return df.eval(term_1 + operator + term_2)

        return reduce(
            lambda x, y: x & y,
            (
                _eval_part(term_1, operator, term_2)
                for term_1, operator, term_2
                in zip(expr_terms, expr_operators, expr_terms[1:])
            )
        )

    def _generate_report(self,
                         summary: List[Tuple[int, int]],
                         nrows: int) -> dict:
        expressions_report = [
            {
                'expression': expr,
                'valid_rows': valid,
                'valid_rows_%': 100.0*valid/nrows,
                'invalid_rows': invalid,
                'invalid_rows_%': 100.0*invalid/nrows
            }
            for expr, (valid, invalid) in zip(self.expressions, summary)
        ]
        return {
            'column_expressions': expressions_report
        }

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        df = self._convert_df_dtypes(df)

        results = (
            self._eval(df, expr)
            for expr in self.expressions
        )
        summary = ((sum(result), sum(~result)) for result in results)
        return self._generate_report(summary, len(df))

    @report(Backend.DASK)
    def _report_dask(self, df: 'dask.dataframe.DataFrame') -> dict:
        df = df.map_partitions(self._convert_df_dtypes,
                               meta=dict(df.dtypes.iteritems()))

        results = (
            dask.dataframe.from_delayed(
                dask.delayed(self._eval)(partition, expr)
                for partition in df.to_delayed()
            )
            for expr in self.expressions
        )
        summary = ((sum(result), sum(~result)) for result in results)
        return self._generate_report(summary, len(df))

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        for item in report['column_expressions']:
            if not item['valid_rows_%'] >= self.at_least_perc:
                return False
            if not item['valid_rows_%'] <= self.at_most_perc:
                return False
        return True
