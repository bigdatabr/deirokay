"""
Statement to check the number unique rows in a scope.
"""
from typing import List

import dask.dataframe  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayStatement
from deirokay.enums import Backend

from ..multibackend import profile, report
from .base_statement import BaseStatement


class Unique(BaseStatement):
    """Checks for the unicity of rows in a scope.

    The only available option is:

    * `at_least_%`: The minimum percentage of unique rows.

    Examples
    --------
    In a table containing information about cities of your country,
    you expect the pair of columns `state` and `city` to be unique
    across all rows. It means that, although some values of `state`
    can be repeated, as well as `city` names, the combination of
    both columns should be unique.
    You can declare the following validation item to represent this
    rule:

    .. code-block:: json

        {
            "scope": ["state", "city"],
            "statements": [
                {
                    "name": "unique"
                }
            ]
        }
    """

    name = 'unique'
    expected_parameters = ['at_least_%']
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)

    @staticmethod
    def _unique_rows(df):
        """Get number of unique rows in DataFrame"""
        _cols = df.columns.tolist()
        value_counts = df.groupby(_cols, dropna=False)[_cols[0]].size()
        return int(sum(value_counts == 1))

    def _report_common(self, df):
        unique_rows = Unique._unique_rows(df)
        return {
            'unique_rows': unique_rows,
            'unique_rows_%': 100.0*unique_rows/len(df),
        }

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        return self._report_common(df)

    @report(Backend.DASK)
    def _report_dask(self, df: 'dask.dataframe.DataFrame') -> dict:
        return self._report_common(df)

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        return report.get('unique_rows_%') >= self.at_least_perc

    @staticmethod
    def _profile_common(df):
        statement = {
            'type': 'unique',
        }  # type: DeirokayStatement

        unique_rows = Unique._unique_rows(df)
        at_least_perc = 100.0*unique_rows/len(df)

        if at_least_perc == 0.0:
            raise NotImplementedError(
                'Statement is useless when all rows are not unique.'
            )

        if at_least_perc != 100.0:
            statement['at_least_%'] = at_least_perc

        return statement

    @profile(Backend.PANDAS)
    @staticmethod
    def _profile_pandas(df: 'pandas.DataFrame') -> DeirokayStatement:
        return Unique._profile_common(df)

    @profile(Backend.DASK)
    @staticmethod
    def _profile_dask(df: 'dask.dataframe.DataFrame') -> DeirokayStatement:
        return Unique._profile_common(df)
