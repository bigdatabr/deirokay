import inspect
import warnings
from copy import deepcopy
from datetime import datetime
from pprint import pprint
from typing import Optional

import deirokay.statements as core_stmts

from .exceptions import ValidationError
from .fs import FileSystem, LocalFileSystem, fs_factory

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
    stmt_type: core_stmts.Statement = statement.get('type')

    if stmt_type == 'custom':
        location = statement.pop('location')
        CustomStatement = _load_custom_statement(location)
        return CustomStatement(statement, read_from)
    else:
        try:
            return core_statement_classes[stmt_type](statement, read_from)
        except KeyError:
            raise NotImplementedError(
                f'Statement type "{stmt_type}" not implemented.\n'
                f'The available types are {list(core_statement_classes)}'
                ' or `custom` for your own statements.'
            )


def validate(df, *,
             against: Optional[dict] = None,
             against_json: Optional[str] = None,
             save_to: str = None,
             current_date=None,
             raise_exception=True) -> dict:

    if save_to:
        save_to = fs_factory(save_to)
        if isinstance(save_to, LocalFileSystem) and not save_to.isdir():
            raise ValueError('The `save_to` parameter must be an existing'
                             ' directory or an S3 path.')

    if against:
        validation_document = deepcopy(against)
    else:
        validation_document = fs_factory(against_json).read_json()

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
