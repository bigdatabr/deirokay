"""
Functions for data profiling and auto-generation of Deirokay Validation
Documents.
"""

import warnings
from typing import List, Optional

from pandas import DataFrame

from deirokay.backend import detect_backend, multibackend_class_factory
from deirokay.enums import Backend
from deirokay.exceptions import UnsupportedBackend

from .__version__ import __version__
from ._typing import (DeirokayStatement, DeirokayValidationDocument,
                      DeirokayValidationItem)
from .fs import fs_factory
from .statements import STATEMENTS_MAP


def _generate_statements(df_scope: DataFrame,
                         backend: Backend) -> List[DeirokayStatement]:

    statements: List[DeirokayStatement] = []

    for stmt_cls in STATEMENTS_MAP.values():
        try:
            execution_cls = multibackend_class_factory(stmt_cls, backend)
            statement = execution_cls.profile(df_scope)
            statements.append(statement)
        except UnsupportedBackend:
            pass
        except NotImplementedError:
            pass
        except Exception as e:
            columns = list(df_scope.columns)
            warnings.warn(
                f'Unexpected error when profiling scope {columns}'
                f' using {stmt_cls.__name__} statement: {e}\n\n'
                'Please, consider reporting this issue to the '
                'developers.',
                RuntimeWarning
            )
    return statements


def _generate_items(df: DataFrame,
                    backend: Backend) -> List[DeirokayValidationItem]:
    items: List[DeirokayValidationItem] = []

    df_columns = list(df.columns)
    scope__table_stmt = [df_columns] + df_columns

    for scope in scope__table_stmt:
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]
        item = {
            'scope': scope,
            'statements': _generate_statements(df_scope, backend)
        }  # type: DeirokayValidationItem

        if item['statements']:
            items.append(item)

    return items


def profile(df: DataFrame,
            document_name: str,
            save_to: Optional[str] = None) -> DeirokayValidationDocument:
    """Generate a validation document from a given template DataFrame
    using profiling methods for builtin Deirokay statements.
    By default, statement objects are generated for the entire template
    DataFrame (the entire set of columns), and then for each of its
    columns individually.
    This function should be used only as a draft for a validation
    document or as a means to quickly launch a first version with
    minimum efforts.
    The user is encouraged to correct and supplement the generated
    document to better meet their expectations.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to use as template, ideally parsed with Deirokay
        `data_reader`.
    document_name : str
        The validation document name.
    save_to : Optional[str], optional
        Path (lcaol or S3) where to save the validation document to.
        The file format is inferred by the its extension.
        If None, no document will be saved. By default None.

    Returns
    -------
    dict
        The auto-generated validation document as Python `dict`.
    """
    backend = detect_backend(df)
    validation_document = {
        'name': document_name,
        'description': f'Auto generated using Deirokay {__version__}',
        'items': _generate_items(df, backend)
    }  # type: DeirokayValidationDocument

    if save_to:
        fs_factory(save_to).write_dict(validation_document, indent=2)

    return validation_document
