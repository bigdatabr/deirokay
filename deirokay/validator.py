import json
from pprint import pprint

from .exceptions import ValidationError


class Statement:
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
        pass


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


def _process_stmt(statement):
    statement = statement.copy()
    stmts_map = {
        'unique': Unique,
    }
    stmt_type: Statement = statement.get('type')
    try:
        return stmts_map[stmt_type](statement)
    except KeyError:
        raise NotImplementedError(f'Statement {stmt_type} not implemented.')


def validate(df, against: dict, *, save_to=None, raise_exception=True):
    validation_document = against

    for item in against.get('items'):
        scope = item.get('scope')
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]

        for stmt in item.get('statements'):
            report = _process_stmt(stmt)(df_scope)
            stmt['report'] = report

    if save_to:
        save_validation_document(validation_document, save_to)

    if raise_exception:
        try:
            raise_validation(validation_document)
        except Exception:
            pprint(validation_document, )
            raise

    return validation_document


def raise_validation(validation_document):
    for report in validation_document.get('items'):
        result = report.get('result')

        if result != 'pass':
            raise ValidationError('Validation failed')


def save_validation_document(document, save_to):
    print(f'Saving validation document to "{save_to}".')
    with open(save_to, 'w') as fp:
        json.dump(document, fp, indent=4)
