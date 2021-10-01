import importlib
import json
import os
from pprint import pprint
from tempfile import NamedTemporaryFile
from typing import Optional

import deirokay.statements as core_stmts

from .exceptions import ValidationError


def _load_custom_statement(location: str):
    if '::' not in location:
        raise ValueError('You should pass your class location using the '
                         'following pattern:\n'
                         '<.py file location>::<class name>')

    original_file_path, class_name = location.split('::')
    module_path, extension = os.path.splitext(original_file_path)
    module_name = os.path.basename(module_path)

    if extension not in ('.py', '.o'):
        raise ValueError('You should pass a valid Python file')

    if original_file_path.startswith('s3://'):
        import boto3
        import re
        bucket, key = (
            re.findall(r's3:\/\/([\w\-]+)\/([\w\-\/.]+)',
                       original_file_path)[0]
        )
        fp = NamedTemporaryFile(suffix='.py')
        boto3.client('s3').download_fileobj(bucket, key, fp)
        file_path = fp.name
    elif original_file_path.startswith('http'):
        raise NotImplementedError('HTTP-backed statement not implemented')
    else:
        file_path = original_file_path

    print(file_path)
    module_dir = os.path.dirname(file_path)

    os.sys.path.insert(0, module_dir)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    os.sys.path.pop(0)

    if original_file_path.startswith('s3://'):
        fp.close()

    if not issubclass(class_, core_stmts.BaseStatement):
        raise ImportError('Your custom statement should be a subclass of '
                          'BaseStatement')

    return class_


def _process_stmt(statement):
    statement = statement.copy()
    stmt_type: core_stmts.Statement = statement.get('type')

    if stmt_type == 'custom':
        location = statement.get('location')
        CustomStatement = _load_custom_statement(location)
    else:
        CustomStatement = None

    stmts_map = {
        'unique': core_stmts.Unique,
        'not_null': core_stmts.NotNull,
        'custom': CustomStatement,
    }
    try:
        return stmts_map[stmt_type](statement)
    except KeyError:
        raise NotImplementedError(f'Statement type "{stmt_type}" '
                                  'not implemented.')


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
            pprint(validation_document)
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
