from deirokay.statements import BaseStatement


class ThereAreValuesGreaterThanX(BaseStatement):

    expected_parameters = ['x']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.x = self.options.get('x')

    def report(self, df):
        bools = df > self.x
        report = {
            'values_greater_than_x': list(bools[bools.all(axis=1)].index)
        }
        return report

    def result(self, report):
        return len(report.get('values_greater_than_x')) > 0
