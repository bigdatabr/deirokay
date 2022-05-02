"""
Statement to evaluate mathematical expressions againt a set of
columns from a given scope.
"""
import re
from decimal import Decimal

import numpy
from pandas import (DataFrame, Float32Dtype, Float64Dtype, Int32Dtype,
                    Int64Dtype, Series)

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
        'expressions', 'at_least_%', 'at_most_%', 'rtol', 'atol'
    ]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.expressions = self.options['expressions']
        self.at_least_perc = self.options.get('at_least_%', 100.0)
        self.at_most_perc = self.options.get('at_most_%', 100.0)
        self.rtol = self.options.get('rtol', 1e-5)
        self.atol = self.options.get('atol', 1e-8)

        if isinstance(self.expressions, str):
            self.expressions = [self.expressions]

        self.valid_expressions = ['==', '!=', '=~', '>=', '<=', '>', '<']
        for expr in self.expressions:
            if not any([x in expr for x in self.valid_expressions]):
                raise SyntaxError('Expression comparison operand not found')

    # docstr-coverage:inherited
    def report(self, df: DataFrame) -> dict:
        report = {}
        df = df.copy()
        df = self._fix_df_dtypes(df)
        for expr in self.expressions:
            row_count = self._eval(df, expr)
            report[expr] = {
                'valid_rows': int((row_count).sum()),
                'valid_rows_%': float(
                    100.0*(row_count).sum()/len(row_count)
                ),
                'invalid_rows': int((~row_count).sum()),
                'invalid_rows_%': float(
                    100.0*(~row_count).sum()/len(row_count)
                )
            }
        return report

    def _fix_df_dtypes(self, df: DataFrame) -> DataFrame:
        """
        Fixes DataFrame dtypes. If Int64Dtype() or Float64Dtype(),
        converts to traditional int64 and float64 dtypes. If object
        dtype, we apply a more accurate check on the column type,
        verifying the dtype of all its cells.

        When a pandas version corrects this bug, we can delete this
        method.
        """
        pandas_dtypes_int = [Int64Dtype(), Int32Dtype()]
        pandas_dtypes_float = [Float64Dtype(), Float32Dtype()]
        pandas_dtypes_decimal = [Decimal]
        for col in df.columns:
            if df[[col]].dtypes.isin(pandas_dtypes_int)[0]:
                df[[col]] = df[[col]].astype(int)
            elif df[[col]].dtypes.isin(pandas_dtypes_float)[0]:
                df[[col]] = df[[col]].astype(float)
            elif df[[col]].dtypes[0] == object:
                col_dtype = list(
                    dict.fromkeys(
                        [type(x) for x in df[col].to_numpy()]
                    )
                )
                if len(col_dtype) > 1:
                    raise Exception('Mixed types in column')
                if col_dtype[0] in pandas_dtypes_decimal:
                    df[[col]] = df[[col]].astype(float)
        return df

    def _eval(self, df: DataFrame, expr: str) -> Series:
        if '=~' not in expr:
            row_count = df.eval(expr)
        else:
            row_count = self._isclose_eval(df, expr)
        return row_count

    def _isclose_eval(self, df: DataFrame, expr: str) -> Series:
        """
        Accomplishes the paper of `pandas.eval` when we have the
        `=~` comparison to evaluate. That implementation is done by
        using `numpy.eval`.
        """
        expr_calculus = re.split('|'.join(self.valid_expressions), expr)
        expr_comparison = re.findall('|'.join(self.valid_expressions), expr)
        expr_comparison.append(None)

        if len(expr_calculus) != len(expr_comparison):
            raise SyntaxError(
                """Invalid expression. Incoherent number of expressions and
                comparison operands"""
            )

        eval_bool = Series(len(df) * [True])

        for i in range(len(expr_calculus) - 1):
            comparison_to_eval = (
                expr_calculus[i] + expr_comparison[i] + expr_calculus[i+1]
            )

            if expr_comparison[i] != '=~':
                eval_bool = df.eval(comparison_to_eval) & eval_bool
            else:
                eval_bool = Series(
                    numpy.isclose(
                        df.eval(expr_calculus[i]),
                        df.eval(expr_calculus[i+1]),
                        atol=self.atol,
                        rtol=self.rtol
                    )
                ) & eval_bool
        return eval_bool

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        for expr in report.keys():
            if not report[expr].get('valid_rows_%') >= self.at_least_perc:
                return False
            if not report[expr].get('valid_rows_%') <= self.at_most_perc:
                return False
        return True
