from deirokay.enums import Backend
from deirokay.statements import BaseStatement
from deirokay.statements.multibackend import report


class ThereAreValuesGreaterThanX(BaseStatement):
    name = 'there_are_values_greater_than_x'
    expected_parameters = ['x']
    supported_backends = [Backend.PANDAS, Backend.DASK]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.x = self.options.get('x')

    def _report_common(self, df):
        bools = df > self.x
        report = {
            'values_greater_than_x': list(bools[bools.all(axis=1)].index)
        }
        return report

    @report(Backend.PANDAS)
    def _report_pandas(self, df):
        return self._report_common(df)

    @report(Backend.DASK)
    def _report_dask(self, df):
        return self._report_common(df)

    def result(self, report):
        return len(report.get('values_greater_than_x')) > 0
