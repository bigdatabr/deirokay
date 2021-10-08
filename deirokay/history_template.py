import json
import os
import warnings

import pandas as pd
import pyjq

from deirokay.config import DEFAULTS

from .fs import split_s3_path


def series_from_disk(series_name, lookback, folder=None):
    if folder is None:
        folder = DEFAULTS['log_folder']

    acc = []
    for parent, _, files in os.walk(os.path.join(folder, series_name)):
        acc += [os.path.join(parent, file) for file in files]

    acc.sort(reverse=True)

    def open_file(file_path):
        with open(file_path) as fp:
            return json.load(fp)

    return [open_file(file) for file in acc[:min(lookback, len(acc))]]


def series_from_s3(series_name, lookback, folder):
    import boto3
    s3 = boto3.client('s3')

    s3_path = os.path.join(folder, series_name)
    bucket, prefix = split_s3_path(s3_path)
    acc = [
        obj['Key']
        for obj in (s3.list_objects(Bucket=bucket, Prefix=prefix)
                    .get('Contents', []))
    ]

    acc.sort(reverse=True)

    def open_file(file_path):
        return json.load(s3.get_object(Bucket=bucket, Key=file_path)['Body'])

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


def get_series(series_name: str, lookback: int,
               read_from: str) -> DocumentNode:
    if read_from.startswith('s3://'):
        docs = series_from_s3(series_name, lookback, read_from)
    else:
        docs = series_from_disk(series_name, lookback, read_from)

    if not docs:
        warnings.warn('No previous log has been found in the specified folder.'
                      ' Make sure you are reading and writing to the right'
                      ' place.', Warning)

    return DocumentNode(docs)
