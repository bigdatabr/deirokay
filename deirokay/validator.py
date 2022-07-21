"""
Set of functions related to Deirokay validation.
"""

import json
import warnings
from copy import deepcopy
from datetime import datetime
from os.path import splitext
from typing import Optional, Union

from jinja2 import BaseLoader
from jinja2 import StrictUndefined as strict
from jinja2.nativetypes import NativeEnvironment

from deirokay._utils import check_columns_in_df_columns, render_dict
from deirokay.backend import detect_backend
from deirokay.enums import SeverityLevel
from deirokay.exceptions import ValidationError
from deirokay.fs import FileSystem, LocalFileSystem, fs_factory
from deirokay.history_template import get_series
from deirokay.statements.loader import statement_factory

from ._typing import (DeirokayDataSource, DeirokayValidationDocument,
                      DeirokayValidationResultDocument)


def validate(
    df: DeirokayDataSource, *,
    against: Union[str, DeirokayValidationDocument],
    save_to: Optional[str] = None,
    save_format: str = None,
    current_date: Optional[datetime] = None,
    raise_exception: bool = True,
    exception_level: SeverityLevel = SeverityLevel.CRITICAL,
    template: Optional[dict] = None
) -> DeirokayValidationResultDocument:
    """Validate a Deirokay DataFrame against a well-defined Validation
    Document.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame preferencially parsed by Deirokay `data_reader`.
    against : Union[str, dict]
        A `dict`-like Validation Document or a local/S3 path to a
        validation file in either YAML or JSON format.
    save_to : str, optional
        Path to folder where the validation result document will be
        saved to. An subfolder named the same as the validation
        document named will be created in this path, and the results
        will be saved inside following the
        `<current_date>.<save_format>` pattern. `<current_date>` is
        formatted as `%Y%m%dT%H%M%S` and `<save_format>` can be either
        `yaml` or `json`.
        If None, no validation log will be saved. By default None.
    save_format : str, optional
        Format in which to save the validation document
        (`yaml` or `json`).
        Only valid when `save_to` is not None.
        If None and `against` is a Python dictionary, defaults to YAML.
        If None and `against` is a valid path to a YAML or JSON file,
        it will keep this format.
        By default None
    current_date : datetime, optional
        Python `datetime.datetime` to use in log file name when saving
        validation result.
        Only valid when `save_to` is not None.
        If None, defaults to `datetime.utcnow()` value and raises a
        warning.
        By default None
    raise_exception : bool, optional
        Whether or not to raise a ValidationError exception whenever a
        statement whose level is greater or equal to `exception_level`
        fails.
        By default True
    exception_level : SeverityLevel, optional
        Minimum statement severity to raise exception for when
        statement validation fails.
        Only valid when `raise_exception` is True.
        By default SeverityLevel.CRITICAL (5).
    template : dict, optional
        Map of custom templates to be replaced in validation document
        before evaluation of statements. For mapped values, if
        callable, the returned value is used instead.

    Returns
    -------
    DeirokayValidationResultDocument
        Validation Result Document dict.

    Raises
    ------
    ValueError
        `save_to` parameter is not a directory neither an S3 path.
    ValidationError
        Validation failed for at least one statement whose severity is
        greater or equal to `exception_level`.
    """
    backend = detect_backend(df)

    if save_to:
        save_to_fs = fs_factory(save_to)
        if isinstance(save_to_fs, LocalFileSystem) and not save_to_fs.isdir():
            raise ValueError('The `save_to` parameter must be an existing'
                             ' directory or an S3 path.')

    if isinstance(against, str):
        save_format = save_format or splitext(against)[1].lstrip('.')
        validation_document = fs_factory(against).read_dict()
    else:
        save_format = save_format or 'yaml'
        validation_document = deepcopy(against)
    assert save_format.lower() in ('json', 'yaml', 'yml'), (
        f'Not a valid format {save_format}'
    )

    # Render templates
    template = dict(
        series=lambda x, y: get_series(x, y, read_from=save_to_fs),
        **(template or {})
    )
    render_dict(NativeEnvironment(loader=BaseLoader(), undefined=strict),
                dict_=validation_document,
                template=template)

    for item in validation_document['items']:
        scope = item['scope']
        scope = [scope] if not isinstance(scope, list) else scope
        check_columns_in_df_columns(scope, df.columns)

        df_scope = df[scope]

        for stmt in item.get('statements'):
            report = statement_factory(stmt, backend)(df_scope)
            if report['result'] is True:
                report['result'] = 'pass'
            else:
                report['result'] = 'fail'
            stmt['report'] = report

    if save_to:
        _save_validation_document(validation_document, save_to_fs,
                                  save_format, current_date)

    if raise_exception:
        raise_validation(validation_document, exception_level)

    return validation_document


def raise_validation(validation_result_document: dict,
                     exception_level: SeverityLevel) -> None:
    """Check for a validation result `dict` and raise a
    `ValidationError` exception whenever a statement whose severity
    level is greater or equal to `exception_level` fails.

    Parameters
    ----------
    validation_result_document : dict
        Validation Result Document generated by a Deirokay validation.
    exception_level : SeverityLevel
        Integer for the minimum severity level to raise exception for.

    Raises
    ------
    ValidationError
        Validation failed for at least one statement whose severity is
        greater or equal to `exception_level`.
    """
    highest_level = None
    for item in validation_result_document['items']:
        scope = item['scope']
        for stmt in item['statements']:
            severity = stmt.get('severity', SeverityLevel.CRITICAL)
            result = stmt['report']['result']

            if result == 'fail':
                if severity >= exception_level:
                    if highest_level is None or severity > highest_level:
                        highest_level = severity
                print(f'Statement failed for scope {scope}:')
                print(json.dumps(stmt, indent=4))

    if highest_level is not None:
        print(f'Severity level threshold was {exception_level}.')
        raise ValidationError(
            highest_level,
            f'Validation failed with severity level {highest_level}.'
        )


def _save_validation_document(document: dict,
                              save_to: FileSystem,
                              save_format: Optional[str] = None,
                              current_date: Optional[datetime] = None) -> None:
    if current_date is None:
        warnings.warn(
            'Document is being saved using the current date returned by the'
            ' `datetime.utcnow()` method. Instead, prefer to explicitly pass a'
            ' `current_date` argument to `validate`.', Warning
        )
        current_date = datetime.utcnow()
    current_date = current_date.strftime('%Y%m%dT%H%M%S')  # type: ignore

    document_name = document['name']
    folder_path = save_to / document_name
    if isinstance(folder_path, LocalFileSystem):
        folder_path.mkdir(parents=True, exist_ok=True)

    file_path = folder_path / f'{current_date}.{save_format}'

    print(f'Saving validation document to "{file_path!s}".')
    file_path.write_dict(document, indent=2)
