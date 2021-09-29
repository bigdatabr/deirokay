

class BaseStatement:
    def __init__(self, stmt_options: dict):
        self.options = stmt_options

    def __call__(self, df):
        internal_report = self.report(df)
        result = self.result(internal_report)

        final_report = {
            'detail': internal_report,
            'result': 'pass' if result else 'fail'
        }
        return final_report

    def report(self, df):
        pass

    def result(self, report):
        return True


Statement = BaseStatement


class Unique(Statement):
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
