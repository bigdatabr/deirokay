"""
Functions for data profiling and auto-generation of Deirokay Validation
Documents.
"""

import warnings
from typing import List, Optional

from pandas import DataFrame

from .__version__ import __version__
from .fs import fs_factory
from .statements import Contain, NotNull, RowCount, Unique

profiling_statement_classes = [
    NotNull, RowCount, Unique, Contain
]


def _generate_items(df: DataFrame) -> List[dict]:
    items = []

    df_columns = list(df.columns)
    scope__table_stmt = zip([df_columns] + df_columns,
                            [True] + [False]*len(df_columns))
    for scope, table_only in scope__table_stmt:
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]
        item = {
            'scope': scope
        }
        items.append(item)
        statements = item['statements'] = []

        for stmt_cls in profiling_statement_classes:
            if table_only and stmt_cls.table_only or not stmt_cls.table_only:
                try:
                    statement = stmt_cls.profile(df_scope)
                    statements.append(statement)
                except NotImplementedError:
                    pass
                except Exception as e:
                    warnings.warn(
                        f'Unexpected error when profiling scope {scope}'
                        f' using {stmt_cls.__name__} statement: {e}\n\n'
                        'Please, consider reporting this issue to the '
                        'developers.',
                        RuntimeWarning
                    )

    return items


def profile(df: DataFrame,
            document_name: str,
            save_to: Optional[str] = None) -> dict:
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
    validation_document = {
        'name': document_name,
        'description': f'Auto generated using Deirokay {__version__}',
    }
    validation_document['items'] = _generate_items(df)

    if save_to:
        fs_factory(save_to).write_dict(validation_document, indent=2)

    return validation_document
