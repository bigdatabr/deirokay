"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Iterable

from pandas import Series


class Validator():
    """Base validation class for column data type validation.
    """

    def __init__(self, *, unique: bool = False, nullable: bool = True):
        self.unique = unique
        self.nullable = nullable

    def __call__(self, listlike: Iterable) -> Series:
        return self.treat(Series(listlike))

    def treat(self, series: Series) -> Series:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Parameters
        ----------
        series : Series
            Raw pandas Series to be treated.

        Raises
        ------
        ValueError
            Column has null values when not_null constraint was
            requested or
            column has duplicate values when unique constraint was
            requested.
        """
        if not self.nullable and any(series.isnull()):
            null_indices = list(series[series.isnull()].index)
            null_indices_limit = null_indices[:min(len(null_indices), 30)]
            raise ValueError(f"The '{series.name}' column has"
                             f" {len(null_indices)} null values,"
                             " but it shouldn't.\n"
                             "Here are the indices of some null values:\n"
                             f"{null_indices_limit}...")

        if self.unique and not series.is_unique:
            duplicated = list(series[series.duplicated(keep='first')])
            duplicated_limit = duplicated[:min(len(duplicated), 10)]

            raise ValueError(f"The '{series.name}' column values"
                             " are not unique, as requested.\n"
                             f"There are {len(duplicated)} non unique values,"
                             " and here are some of them:\n"
                             f"{duplicated_limit}...")

        return series

    @staticmethod
    def serialize(series: Series) -> dict:
        """Create a Deirokay-compatible serializable object that can
        be serialized (in JSON or YAML formats) and parsed back by
        Deirokay treaters.
        This method is useful when generating validation documents
        that embed Deirokay-compatible user data.
        The output format is a Python dict containing two keys:
        - `values`: list of values as Python object.
        - `parser`: Deirokay options to parse the data.
        (See more: `deirokay.parser.get_treater_instance`).

        Parameters
        ----------
        series : Series
            Pandas Series to be serialized.

        Returns
        -------
        dict
            A Python dict containing the keys `values` and `parser`.
        """
        raise NotImplementedError('No serializer for this treater.')
