import importlib
import json
import os
import warnings
from copy import deepcopy
from datetime import datetime
from pathlib import Path
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


def _process_stmt(statement, read_from=None):
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
        'row_count': core_stmts.RowCount,
    }
    try:
        return stmts_map[stmt_type](statement, read_from)
    except KeyError:
        raise NotImplementedError(f'Statement type "{stmt_type}" '
                                  'not implemented.')


def validate(df, *,
             against: Optional[dict] = None,
             against_json: Optional[str] = None,
             save_to=None,
             current_date=None,
             raise_exception=True) -> dict:
    if save_to and not os.path.isdir(save_to):
        raise ValueError('The `save_to` parameter must be an existing'
                         ' directory')

    if against:
        validation_document = deepcopy(against)
    else:
        with open(against_json) as fp:
            validation_document = json.load(fp)

    for item in validation_document.get('items'):
        scope = item.get('scope')
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]

        for stmt in item.get('statements'):
            report = _process_stmt(stmt, read_from=save_to)(df_scope)
            stmt['report'] = report

    if save_to:
        _save_validation_document(validation_document, save_to, current_date)

    if raise_exception:
        try:
            _raise_validation(validation_document)
        except Exception:
            pprint(validation_document)
            raise

    return validation_document


def _raise_validation(validation_document):
    for item in validation_document.get('items'):
        for stmt in item.get('statements'):
            result = stmt.get('report').get('result')

            if result != 'pass':
                raise ValidationError('Validation failed')


def _save_validation_document(document, save_to, current_date):
    if current_date is None:
        warnings.warn(
            'Document is being saved using the current date returned by the'
            ' `datetime.utcnow()` method. Instead, prefer to explicitly pass a'
            ' `current_date` argument to `validate`.'
        )
        current_date = datetime.utcnow()
    current_date = current_date.strftime('%Y%m%dT%H%M%S')

    document_name = document['name']
    folder_path = Path(save_to, document_name)
    folder_path.mkdir(parents=True, exist_ok=True)

    file_path = folder_path / f'{current_date}.json'

    print(f'Saving validation document to "{file_path!s}".')
    with open(file_path, 'w') as fp:
        json.dump(document, fp, indent=4)
