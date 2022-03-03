from .base_statement import BaseStatement


class Unique(BaseStatement):
    """Check if the rows of a scoped DataFrame are unique."""

    name = 'unique'
    expected_parameters = ['at_least_%']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.at_least_perc = self.options.get('at_least_%', 100.0)

    # docstr-coverage:inherited
    def report(self, df):
        unique = ~df.duplicated(keep=False)

        report = {
            'unique_rows': int(unique.sum()),
            'unique_rows_%': float(100.0*unique.sum()/len(unique)),
        }
        return report

    # docstr-coverage:inherited
    def result(self, report):
        return report.get('unique_rows_%') >= self.at_least_perc

    # docstr-coverage:inherited
    @staticmethod
    def profile(df):
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
