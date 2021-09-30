import json
from pprint import pprint
from typing import Optional

import deirokay.statements as core_stmts

from .exceptions import ValidationError


def _process_stmt(statement):
    statement = statement.copy()
    stmts_map = {
        'unique': core_stmts.Unique,
    }
    stmt_type: core_stmts.Statement = statement.get('type')
    try:
        return stmts_map[stmt_type](statement)
    except KeyError:
        raise NotImplementedError(f'Statement {stmt_type} not implemented.')


def validate(df, *,
             against: Optional[dict] = None,
             against_json: Optional[str] = None,
             save_to=None,
             raise_exception=True) -> dict:
    if against:
        validation_document = against
    else:
        with open(against_json) as fp:
            validation_document = json.load(fp)

    for item in validation_document.get('items'):
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
    for item in validation_document.get('items'):
        for stmt in item.get('statements'):
            result = stmt.get('report').get('result')

            if result != 'pass':
                raise ValidationError('Validation failed')


def save_validation_document(document, save_to):
    print(f'Saving validation document to "{save_to}".')
    with open(save_to, 'w') as fp:
        json.dump(document, fp, indent=4)
