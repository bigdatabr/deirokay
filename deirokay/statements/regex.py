import re

from .base_statement import BaseStatement


class Regex(BaseStatement):
    """Check if the rows of a scoped DataFrame obey to a regex pattern."""

    name = 'regex'
    expected_parameters = ['pattern', 'at_least_%']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pattern = self.options.get('pattern', '.')
        self.at_least_perc = self.options.get('at_least_%', 100.0)

    def report(self, df):

        report = {}

        for column in df.columns:
            matches = df[column].apply(lambda x: bool(
                                       re.fullmatch(self.pattern, str(x))))
            report[column] = {
                'matching_rows': int(matches.sum()),
                'matching_rows_%': float(100*matches.sum()/len(matches)),
                'not_matching_rows': int((~matches).sum()),
                'not_matching_rows_%': float(100.0*(~matches).sum(
                                                              )/len(matches)),
            }

        # print('report:\n', pd.DataFrame(report).to_markdown())

        return report

    def result(self, report):
        for col_report in report:
            if not report[col_report].get('matching_rows_%'
                                          ) >= self.at_least_perc:
                return False
        return True
