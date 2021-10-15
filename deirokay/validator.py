import inspect
import warnings
from copy import deepcopy
from datetime import datetime
import json
from typing import Optional, Union

import deirokay.statements as core_stmts

from .exceptions import ValidationError
from .fs import FileSystem, LocalFileSystem, fs_factory
from .enums import Level

# List all Core statement classes automatically
core_statement_classes = {
    cls.name: cls
    for _, cls in inspect.getmembers(core_stmts)
    if isinstance(cls, type) and
    issubclass(cls, core_stmts.BaseStatement) and
    cls is not core_stmts.BaseStatement
}


def _load_custom_statement(location: str):
    if '::' not in location:
        raise ValueError('You should pass your class location using the'
                         ' following pattern:\n'
                         '<.py file location>::<class name>')

    file_path, class_name = location.split('::')

    fs = fs_factory(file_path)
    module = fs.import_as_python_module()
    cls = getattr(module, class_name)

    if not issubclass(cls, core_stmts.BaseStatement):
        raise ImportError('Your custom statement should be a subclass of '
                          'BaseStatement')

    return cls


def _process_stmt(statement, read_from: FileSystem = None):
    stmt_type = statement.pop('type')
    severity = statement.pop('severity', None)
    location = statement.pop('location', None)

    if stmt_type == 'custom':
        if not location:
            raise KeyError('A custom statement must define a `location`'
                           ' parameter.')
        cls = _load_custom_statement(location)
    elif stmt_type in core_statement_classes:
        cls = core_statement_classes[stmt_type]
    else:
        raise NotImplementedError(
            f'Statement type "{stmt_type}" not implemented.\n'
            f'The available types are {list(core_statement_classes)}'
            ' or `custom` for your own statements.'
        )

    statement_instance = cls(statement, read_from)

    statement['type'] = stmt_type
    if severity:
        statement['severity'] = severity
    if location:
        statement['location'] = location

    return statement_instance


def validate(df, *,
             against: Union[str, dict],
             save_to: Optional[str] = None,
             current_date: Optional[datetime] = None,
             raise_exception: bool = True,
             exception_level: int = Level.CRITICAL) -> dict:

    if save_to:
        save_to = fs_factory(save_to)
        if isinstance(save_to, LocalFileSystem) and not save_to.isdir():
            raise ValueError('The `save_to` parameter must be an existing'
                             ' directory or an S3 path.')

    if isinstance(against, str):
        validation_document = fs_factory(against).read_json()
    else:
        validation_document = deepcopy(against)

    for item in validation_document.get('items'):
        scope = item.get('scope')
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]

        for stmt in item.get('statements'):
            report = _process_stmt(stmt, read_from=save_to)(df_scope)
            if report['result'] is True:
                report['result'] = 'pass'
            else:
                report['result'] = 'fail'
            stmt['report'] = report

    if save_to:
        _save_validation_document(validation_document, save_to, current_date)

    if raise_exception:
        raise_validation(validation_document, exception_level)

    return validation_document


def raise_validation(validation_document, exception_level):
    highest_level = None
    for item in validation_document.get('items'):
        for stmt in item.get('statements'):
            severity = stmt.get('severity', Level.CRITICAL)
            result = stmt.get('report').get('result')

            if result != 'pass':
                if severity >= exception_level:
                    if highest_level is None or severity > highest_level:
                        highest_level = severity
                    print('Validation failed (suppressed)')
                print('Validation failed:')
                print(json.dumps(stmt, indent=4))

    if highest_level is not None:
        raise ValidationError(highest_level, 'Validation failed')


def _save_validation_document(document: dict,
                              save_to: FileSystem,
                              current_date: Optional[datetime] = None):
    if current_date is None:
        warnings.warn(
            'Document is being saved using the current date returned by the'
            ' `datetime.utcnow()` method. Instead, prefer to explicitly pass a'
            ' `current_date` argument to `validate`.', Warning
        )
        current_date = datetime.utcnow()
    current_date = current_date.strftime('%Y%m%dT%H%M%S')

    document_name = document['name']
    folder_path = save_to/document_name
    if isinstance(folder_path, LocalFileSystem):
        folder_path.mkdir(parents=True, exist_ok=True)

    file_path = folder_path/f'{current_date}.json'

    print(f'Saving validation document to "{file_path!s}".')
    file_path.write_json(document, indent=1)
