from typing import Optional

import pandas as pd

from .__version__ import __version__
from .fs import fs_factory
from .statements import NotNull, RowCount, Unique

profiling_statement_classes = [
    NotNull, RowCount, Unique
]


def _generate_items(df):
    items = []

    df_columns = list(df.columns)
    scope__all_columns = zip([df_columns] + df_columns,
                             [True] + [False]*len(df_columns))
    for scope, all_columns in scope__all_columns:
        df_scope = df[scope] if isinstance(scope, list) else df[[scope]]
        item = {
            'scope': scope
        }
        items.append(item)
        statements = item['statements'] = []

        for stmt_cls in profiling_statement_classes:
            try:
                statement = stmt_cls.profile(df_scope, all_columns=all_columns)
                statements.append(statement)
            except NotImplementedError:
                pass

    return items


def profile(df: pd.DataFrame, document_name: str,
            save_to: Optional[str] = None):
    validation_document = {
        'name': document_name,
        'description': f'Auto generated using Deirokay {__version__}',
    }
    validation_document['items'] = _generate_items(df)

    if save_to:
        fs_factory(save_to).write_json(validation_document, indent=2)

    return validation_document
