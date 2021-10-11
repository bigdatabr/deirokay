import warnings

import pandas as pd
import pyjq

from .fs import FileSystem


def series_from_fs(series_name: str, lookback: int, folder: FileSystem):

    acc = (folder/series_name).ls(recursive=True, files_only=True)

    acc.sort(reverse=True)
    acc = acc[:min(lookback, len(acc))]

    return [file.read_json() for file in acc]


class NullCallableNode():
    def __getattr__(self, name):
        return lambda: None


class StatementNode():
    def __init__(self, statements):
        attributes = pyjq.all('.[].report.detail | keys', statements)
        attributes = set([key for sub in attributes for key in sub])

        for att in attributes:
            child = pyjq.all(f'.[].report.detail.{att}', statements)
            setattr(self, att, pd.Series(child))

    def __getattr__(self, name):
        return NullCallableNode()


class ItemNode():
    def __init__(self, items):
        attributes = set(pyjq.all(
            '.[].statements[] | if .alias != null then .alias else .type end',
            items
        ))

        for att in attributes:
            child = pyjq.all(
                '.[].statements[] | '
                f'select(.alias == "{att}" or .type == "{att}")',
                items
            )
            setattr(self, att, StatementNode(child))

    def __getattr__(self, name):
        return StatementNode([])


class DocumentNode():
    def __init__(self, docs):
        attributes = set(pyjq.all(
            '.[].items[] | if .alias != null then .alias else .scope end',
            docs
        ))

        for att in attributes:
            child = pyjq.all(
                '.[].items[] | '
                f'select(.alias == "{att}" or .scope == "{att}")',
                docs
            )
            setattr(self, att, ItemNode(child))

    def __getattr__(self, name):
        return ItemNode([])


def get_series(series_name: str, lookback: int,
               read_from: FileSystem) -> DocumentNode:
    docs = series_from_fs(series_name, lookback, read_from)

    if not docs:
        warnings.warn('No previous log has been found in the specified folder.'
                      ' Make sure you are reading and writing to the right'
                      ' place.', Warning)

    return DocumentNode(docs)
