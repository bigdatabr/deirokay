from typing import Optional

import pandas as pd
from jinja2 import BaseLoader
from jinja2.nativetypes import NativeEnvironment

from .history_template import get_series


class BaseStatement:
    expected_parameters = ['type', 'location']
    jinjaenv = NativeEnvironment(loader=BaseLoader())

    def __init__(self, options: dict, read_from: Optional[str] = None):
        self._validate_options(options)
        self.options = options
        self._read_from = read_from
        self._parse_options()

    def _validate_options(self, options: dict):
        cls = type(self)
        unexpected_parameters = [
            option for option in options
            if option not in (cls.expected_parameters
                              + BaseStatement.expected_parameters)
        ]
        if unexpected_parameters:
            raise ValueError(
                f'Invalid parameters passed to {cls.__name__} statement: '
                f'{unexpected_parameters}\n'
                f'The valid parameters are: {cls.expected_parameters}'
            )

    def _parse_options(self):
        for key, value in self.options.items():
            if isinstance(value, str):
                rendered = (
                    BaseStatement.jinjaenv.from_string(value)
                    .render(
                        series=lambda x, y: get_series(x, y, self._read_from)
                    )
                )
                self.options[key] = rendered

    def __call__(self, df: pd.DataFrame):
        internal_report = self.report(df)
        result = self.result(internal_report)

        final_report = {
            'detail': internal_report,
            'result': 'pass' if result else 'fail'
        }
        return final_report

    def report(self, df: pd.DataFrame) -> dict:
        """
            Receive a DataFrame containing only columns on the scope of
            validation and returns a report of related metrics that can
            be used later to declare this Statement as fulfilled or
            failed.
        """
        return {}

    def result(self, report: dict) -> bool:
        """
            Receive the report previously generated and declare this
            statement as either fulfilled (True) or failed (False).
        """
        return True


Statement = BaseStatement


class Unique(Statement):
    expected_parameters = ['at_least_%']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)

    def report(self, df):
        unique = ~df.duplicated(keep=False)

        report = {
            'unique_rows': int(unique.sum()),
            'unique_rows_%': float(100.0*unique.sum()/len(unique)),
        }
        return report

    def result(self, report):
        return report.get('unique_rows_%') > self.at_least_perc


class NotNull(Statement):
    expected_parameters = ['at_least_%', 'at_most_%', 'multicolumn_logic']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)
        self.at_most_perc = self.options.get('at_most_%', 100.0)
        self.multicolumn_logic = self.options.get('multicolumn_logic', 'any')

        assert self.multicolumn_logic in ('any', 'all')

    def report(self, df):
        if self.multicolumn_logic == 'all':
            not_nulls = ~df.isnull().all(axis=1)
        else:
            not_nulls = ~df.isnull().any(axis=1)

        report = {
            'null_rows': int((~not_nulls).sum()),
            'null_rows_%': float(100.0*(~not_nulls).sum()/len(not_nulls)),
            'not_null_rows': int(not_nulls.sum()),
            'not_null_rows_%': float(100.0*not_nulls.sum()/len(not_nulls)),
        }
        return report

    def result(self, report):
        if not report.get('not_null_rows_%') >= self.at_least_perc:
            return False
        if not report.get('not_null_rows_%') <= self.at_most_perc:
            return False
        return True


class RowCount(Statement):
    expected_parameters = ['min', 'max']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.min = self.options.get('min', None)
        self.max = self.options.get('max', None)

    def report(self, df):
        row_count = len(df)

        report = {
            'rows': row_count,
        }
        return report

    def result(self, report):
        row_count = report['rows']

        if self.min is not None:
            if not row_count >= self.min:
                return False
        if self.max is not None:
            if not row_count <= self.max:
                return False
        return True
