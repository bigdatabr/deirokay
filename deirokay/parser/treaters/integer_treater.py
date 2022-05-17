"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from pandas import NA, Series

from .numeric_treater import NumericTreater


class IntegerTreater(NumericTreater):
    """Treater for integer variables"""

    # docstr-coverage:inherited
    def _treat_pandas(self, series: Series) -> Series:
        return super()._treat_pandas(series).astype(float).astype('Int64')

    # docstr-coverage:inherited
    @staticmethod
    def _serialize_pandas(series: Series) -> dict:
        def _convert(item):
            if item is NA:
                return None
            return int(item)
        return {
            'values': [_convert(item) for item in series],
            'parser': {
                'dtype': 'integer'
            }
        }
