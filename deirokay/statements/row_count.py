from .base_statement import BaseStatement


class RowCount(BaseStatement):
    """Check if the number of rows in a DataFrame is within a
    range."""

    name = 'row_count'
    expected_parameters = ['min', 'max', 'distinct']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.min = self.options.get('min', None)
        self.max = self.options.get('max', None)
        self.distinct = self.options.get('distinct', False)

    # docstr-coverage:inherited
    def report(self, df):
        row_count = len(df)
        distinct_count = len(df.drop_duplicates())

        report = {
            'rows': row_count,
            'distinct_rows': distinct_count,
        }
        return report

    # docstr-coverage:inherited
    def result(self, report):
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

    # docstr-coverage:inherited
    @staticmethod
    def profile(df):
        if len(df.columns) > 1:
            count = len(df)
            return {
                'type': 'row_count',
                'min': count,
                'max': count
            }
        else:
            count = len(df.drop_duplicates())
            return {
                'type': 'row_count',
                'distinct': True,
                'min': count,
                'max': count
            }
