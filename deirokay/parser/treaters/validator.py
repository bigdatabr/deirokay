"""
Classes and functions to treat column data types according to
Deirokay data types.
"""

from typing import Iterable, Union

import dask.dataframe as dd
from pandas import Series

from deirokay.backend import Backend, detect_backend


class Validator():
    """Base validation class for column data type validation.
    """

    def __init__(self, *, unique: bool = False, nullable: bool = True):
        self.unique = unique
        self.nullable = nullable

    def __call__(self, listlike: Iterable) -> Series:
        return self.treat(Series(listlike))

    def treat(self, series: Iterable) -> Union[Series, dd.Series]:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Calls the appropriate method for the detected backend

        Parameters
        ----------
        series : Iterable
            Raw series to be treated.

        """

        try:
            backend = detect_backend(series)
        except ValueError:
            backend = Backend.PANDAS

        try:
            return getattr(self, f'_treat_{backend.value}')(series)
        except AttributeError:
            raise NotImplementedError(
                f'Treater not implemented for backend {backend.value}'
            )

    def _treat_pandas(self, series: Iterable) -> Series:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Parameters
        ----------
        series : Iterable
            Raw pandas Series to be treated.

        Raises
        ------
        ValueError
            Column has null values when not_null constraint was
            requested or
            column has duplicate values when unique constraint was
            requested.
        """
        series = Series(series)
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

    def _treat_dask(self, series: dd.Series) -> dd.Series:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Parameters
        ----------
        series : Iterable
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

            duplicated = list(series[series.duplicated(keep='first')])
            duplicated_limit = duplicated[:min(len(duplicated), 10)]

            raise ValueError(f"The '{series.name}' column values"
                             " are not unique, as requested.\n"
                             f"There are {len(duplicated)} non unique values,"
                             " and here are some of them:\n"
                             f"{duplicated_limit}...")

        return series

    @classmethod
    def serialize(cls, series: Series) -> dict:
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
        try:
            backend = detect_backend(series)
        except ValueError:
            backend = Backend.PANDAS

        try:
            return getattr(cls, f'_serialize_{backend.value}')(series)
        except AttributeError:
            raise NotImplementedError(
                f'Serializer not implemented for backend {backend.value}'
            )
