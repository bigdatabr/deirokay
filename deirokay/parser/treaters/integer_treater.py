"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from pandas import NA, Series

from .numeric_treater import NumericTreater


class IntegerTreater(NumericTreater):
    """Treater for integer variables"""

    # docstr-coverage:inherited
    def treat(self, series: Series) -> Series:
        return super().treat(series).astype(float).astype('Int64')

    # docstr-coverage:inherited
    @staticmethod
    def serialize(series: Series) -> dict:
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
