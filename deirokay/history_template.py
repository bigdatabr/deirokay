import json
import os
from functools import wraps
from os.path import join

import pandas as pd
import pyjq

from deirokay.config import DEFAULTS


def series_from_disk(series_name, lookback, folder=None):
    if folder is None:
        folder = DEFAULTS['log_folder']

    acc = []
    for parent, folders, files in os.walk(join(folder, series_name)):
        acc += [join(parent, file) for file in files]

    acc.sort(reverse=True)

    def open_file(file_path):
        with open(file_path) as fp:
            return json.load(fp)

    return [open_file(file) for file in acc[:min(lookback, len(acc))]]


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


def get_attribute_soft(getattribute):
    @wraps(getattribute)
    def wrapper(self, name):
        try:
            return getattribute(name)
        except AttributeError:
            return None
    return wrapper


def get_series(series_name: str, lookback: int) -> DocumentNode:
    docs = series_from_disk(series_name, lookback)

    return DocumentNode(docs)
