from .base_statement import BaseStatement


class NotNull(BaseStatement):
    """Check if the rows of a scoped DataFrame are not null."""

    name = 'not_null'
    expected_parameters = ['at_least_%', 'at_most_%', 'multicolumn_logic']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)
        self.at_most_perc = self.options.get('at_most_%', 100.0)
        self.multicolumn_logic = self.options.get('multicolumn_logic', 'any')

        assert self.multicolumn_logic in ('any', 'all')

    # docstr-coverage:inherited
    def report(self, df):
        if self.multicolumn_logic == 'all':
            #  REMINDER: ~all == any
            not_nulls = ~df.isnull().any(axis=1)
        else:
            not_nulls = ~df.isnull().all(axis=1)

        report = {
            'null_rows': int((~not_nulls).sum()),
            'null_rows_%': float(100.0*(~not_nulls).sum()/len(not_nulls)),
            'not_null_rows': int(not_nulls.sum()),
            'not_null_rows_%': float(100.0*not_nulls.sum()/len(not_nulls)),
        }
        return report

    # docstr-coverage:inherited
    def result(self, report):
        if not report.get('not_null_rows_%') >= self.at_least_perc:
            return False
        if not report.get('not_null_rows_%') <= self.at_most_perc:
            return False
        return True

    # docstr-coverage:inherited
    @staticmethod
    def profile(df):
        not_nulls = ~df.isnull().all(axis=1)

        statement = {
            'type': 'not_null'
        }

        at_least_perc = float(100.0*not_nulls.sum()/len(not_nulls))

        if at_least_perc == 0.0:
            raise NotImplementedError(
                'Statement is useless when all rows are null.'
            )

        if at_least_perc != 100.0:
            statement['at_least_%'] = at_least_perc

        return statement
