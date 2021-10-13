from os.path import splitext
from typing import Union

import pandas as pd

from ..enums import DTypes
from ..fs import fs_factory
from .treaters import data_treater


def data_reader(file_path: str, options: Union[dict, str]):
    if isinstance(options, str):
        options = fs_factory(options).read_json()

    for column in options.get('columns').values():
        column['dtype'] = DTypes(column['dtype'])

    columns = options.pop('columns')

    df = pandas_read(file_path, **options)
    data_treater(df, columns)

    return df


def pandas_read(file_path, **kwargs):
    file_extension = splitext(file_path)[1].lstrip('.')

    pandas_kwargs = {}
    default_args_by_extension = {
        'csv': {
            'dtype': str,
            'skipinitialspace': True,
        }
    }
    pandas_kwargs.update(default_args_by_extension.get(file_extension, {}))
    pandas_kwargs.update(kwargs)

    pd_read_func = getattr(pd, f'read_{file_extension}', None)
    if pd_read_func is None:
        other_readers = {'xls': pd.read_excel}
        try:
            pd_read_func = other_readers[file_extension]
        except KeyError:
            raise TypeError('File type not supported')

    return pd_read_func(file_path, **pandas_kwargs)
