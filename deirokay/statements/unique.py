"""
Statement to check the number unique rows in a scope.
"""
from pandas import DataFrame

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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)

    # docstr-coverage:inherited
    def report(self, df: DataFrame) -> dict:
        unique = ~df.duplicated(keep=False)

        report = {
            'unique_rows': int(unique.sum()),
            'unique_rows_%': float(100.0*unique.sum()/len(unique)),
        }
        return report

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        return report.get('unique_rows_%') >= self.at_least_perc

    # docstr-coverage:inherited
    @staticmethod
    def profile(df: DataFrame) -> dict:
        unique = ~df.duplicated(keep=False)

        statement = {
            'type': 'unique',
        }

        at_least_perc = float(100.0*unique.sum()/len(unique))

        if at_least_perc == 0.0:
            raise NotImplementedError(
                'Statement is useless when all rows are not unique.'
            )

        if at_least_perc != 100.0:
            statement['at_least_%'] = at_least_perc

        return statement
