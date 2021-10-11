import warnings

import jq
import pandas as pd

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
    detail_keys = jq.compile('.[].report.detail | keys')

    def __init__(self, statements):
        attributes = StatementNode.detail_keys.input(statements).all()
        attributes = set([key for sub in attributes for key in sub])

        for att in attributes:
            child = (
                jq.compile(f'.[].report.detail.{att}').input(statements).all()
            )
            setattr(self, att, pd.Series(child))

    def __getattr__(self, name):
        return NullCallableNode()


class ItemNode():
    attribute_keys = jq.compile(
        '.[].statements[] | if .alias != null then .alias else .type end'
    )

    def __init__(self, items):
        attributes = set(ItemNode.attribute_keys.input(items).all())

        for att in attributes:
            child = jq.compile(
                '.[].statements[] | '
                f'select(.alias == "{att}" or .type == "{att}")'
            ).input(items).all()
            setattr(self, att, StatementNode(child))

    def __getattr__(self, name):
        return StatementNode([])


class DocumentNode():
    attribute_keys = jq.compile(
        '.[].items[] | if .alias != null then .alias else .scope end'
    )

    def __init__(self, docs):
        attributes = set(DocumentNode.attribute_keys.input(docs).all())

        for att in attributes:
            child = jq.compile(
                '.[].items[] | '
                f'select(.alias == "{att}" or .scope == "{att}")'
            ).input(docs).all()
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
