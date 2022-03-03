"""
Module for BaseStatement and builtin Deirokay statements.
"""

import re
from decimal import Decimal

import numpy
import pandas
from pandas import Float64Dtype, Int64Dtype, Series

from .base_statement import BaseStatement


class ColumnExpression(BaseStatement):
    """
    Evaluates an expression involving the scope columns, using
    `numpy.eval()`
    """

    name = 'column_expression'
    expected_parameters = [
        'expressions', 'at_least_%', 'at_most_%', 'rtol', 'atol'
    ]
    table_only = False

    def __init__(self, *args, **kwargs):
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
    def report(self, df):
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

    def _fix_df_dtypes(self, df):
        """
        Fixes DataFrame dtypes. If Int64Dtype() or Float64Dtype(),
        converts to traditional int64 and float64 dtypes. If object
        dtype, we apply a more accurate check on the column type,
        verifying the dtype of all its cells.

        When a pandas version corrects this bug, we can delete this
        method.
        """
        pandas_dtypes_int = [Int64Dtype(), pandas.Int32Dtype()]
        pandas_dtypes_float = [Float64Dtype(), pandas.Float32Dtype()]
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

    def _eval(self, df, expr):
        if '=~' not in expr:
            row_count = df.eval(expr)
        else:
            row_count = self._isclose_eval(df, expr)
        return row_count

    def _isclose_eval(self, df, expr):
        """
        Accomplishes the paper of `pandas.eval` when we have the
        =~ comparison to evaluate. That implementation is done by
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
    def result(self, report):
        for expr in report.keys():
            if not report[expr].get('valid_rows_%') >= self.at_least_perc:
                return False
            if not report[expr].get('valid_rows_%') <= self.at_most_perc:
                return False
        return True
