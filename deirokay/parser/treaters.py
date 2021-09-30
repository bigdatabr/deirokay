import pandas as pd

from ..enums import DTypes
from .validator import Validator


def data_treater(df, options):
    treat_dtypes = {
        DTypes.INT64: IntegerTreater,
        DTypes.DATETIME: DateTime64Treater,
        DTypes.FLOAT64: FloatTreater,
        DTypes.STRING: StringTreater,
        DTypes.TIME: TimeTreater,
    }
    for col, option in options.items():
        option: dict = option.copy()

        dtype = option.pop('dtype', None)
        rename_to = option.pop('rename', None)

        if dtype is not None:
            try:
                df[col] = treat_dtypes[dtype](**option)(df[col])
            except KeyError:
                raise NotImplementedError(f"Handler for '{dtype}' hasn't been"
                                          " implemented yet")

        if rename_to is not None:
            df.rename(columns={col: rename_to}, inplace=True)


class NumericTreater(Validator):
    def __init__(self, thousands_sep=None, **kwargs):
        super().__init__(**kwargs)

        self.thousands_sep = thousands_sep

    def __call__(self, series):
        super().__call__(series)

        if self.thousands_sep is not None:
            series = series.str.replace(self.thousands_sep, '', regex=False)

        return series


class FloatTreater(NumericTreater):
    def __init__(self, decimal_sep='.', **kwargs):
        super().__init__(**kwargs)

        self.decimal_sep = decimal_sep

    def __call__(self, series):
        series = super().__call__(series)

        if self.decimal_sep is not None:
            series = series.str.replace(self.decimal_sep, '.', regex=False)

        return series.astype(float).astype('Float64')


class IntegerTreater(NumericTreater):
    def __call__(self, series):
        return super().__call__(series).astype(float).astype('Int64')


class DateTime64Treater(Validator):
    def __init__(self, format='%Y-%m-%d %H:%M:%S', **kwargs):
        super().__init__(**kwargs)

        self.format = format

    def __call__(self, series):
        super().__call__(series)

        return pd.to_datetime(series, format=self.format)


class TimeTreater(DateTime64Treater):
    def __init__(self, format='%H:%M:%S', **kwargs):
        super().__init__(format, **kwargs)

    def __call__(self, series):
        return super().__call__(series).dt.time


class StringTreater(Validator):
    def __init__(self, treat_null_as=None, **kwargs):
        super().__init__(**kwargs)

        self.treat_null_as = treat_null_as

    def __call__(self, series):
        super().__call__(series)

        if self.treat_null_as is not None:
            series = series.fillna(self.treat_null_as)

        return series
