import json
from os.path import splitext

import pandas as pd

from ..enums import DTypes
from .treaters import data_treater


def data_reader(file_path, options={}, options_json=None, **kwargs):
    df = pandas_read(file_path, **kwargs)

    if options:
        data_treater(df, options)
    elif options_json:
        with open(options_json) as fp:
            options = json.load(fp)
        for option in options.values():
            option['dtype'] = DTypes(option['dtype'])
        data_treater(df, options)

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
