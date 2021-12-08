from .base_statement import BaseStatement


class RowCount(BaseStatement):
    """Check if the number of rows in a DataFrame is within a
    range."""

    name = 'row_count'
    expected_parameters = ['min', 'max']
    table_only = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.min = self.options.get('min', None)
        self.max = self.options.get('max', None)

    # docstr-coverage:inherited
    def report(self, df):
        row_count = len(df)

        report = {
            'rows': row_count,
        }
        return report

    # docstr-coverage:inherited
    def result(self, report):
        row_count = report['rows']

        if self.min is not None:
            if not row_count >= self.min:
                return False
        if self.max is not None:
            if not row_count <= self.max:
                return False
        return True

    # docstr-coverage:inherited
    @staticmethod
    def profile(df):
        row_count = len(df)

        statement = {
            'type': 'row_count',
            'min': row_count,
            'max': row_count,
        }
        return statement
