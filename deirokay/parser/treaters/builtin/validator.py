"""
Classes and functions to treat column data types according to
Deirokay data types.
"""
import dask.dataframe
import pandas

from deirokay.enums import Backend

from ..multibackend import treat
from .base_treater import BaseTreater


class Validator(BaseTreater):
    """Base validation class for column data type validation."""
    supported_backends = [Backend.PANDAS, Backend.DASK]

    DISPLAY_NULL_INDICES_LIMIT = 30
    DISPLAY_DUPL_INDICES_LIMIT = 10

    def __init__(self, *,
                 unique: bool = False,
                 nullable: bool = True):
        self.unique = unique
        self.nullable = nullable

    @treat(Backend.PANDAS)
    def _treat_pandas(self, series: 'pandas.Series', /) -> 'pandas.Series':
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
            null_indices_limit = null_indices[:min(
                len(null_indices), self.DISPLAY_NULL_INDICES_LIMIT
            )]
            raise ValueError(
                f"The '{series.name}' column has {len(null_indices)} null"
                " values, but it shouldn't.\n"
                "Here are the indices of some null values:\n"
                f"{null_indices_limit}..."
            )

        if self.unique and not series.is_unique:
            duplicated = list(series[series.duplicated(keep='first')])
            duplicated_limit = duplicated[:min(
                len(duplicated), self.DISPLAY_DUPL_INDICES_LIMIT
            )]
            raise ValueError(
                f"The '{series.name}' column values are not unique, as"
                " requested.\n"
                f"There are {len(duplicated)} non unique values, and here are"
                " some of them:\n"
                f"{duplicated_limit}..."
            )

        return series

    @treat(Backend.DASK)
    def _treat_dask(
        self, series: 'dask.dataframe.Series'
    ) -> 'dask.dataframe.Series':
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Parameters
        ----------
        series : Iterable
            Raw dask Series to be treated.

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
            null_indices_limit = null_indices[:min(
                len(null_indices), self.DISPLAY_NULL_INDICES_LIMIT
            )]
            raise ValueError(
                f"The '{series.name}' column has {len(null_indices)} null"
                " values, but it shouldn't.\n"
                "Here are the indices of some null values:\n"
                f"{null_indices_limit}..."
            )

        if self.unique and not series.is_unique:
            duplicated = list(series[series.duplicated(keep='first')])
            duplicated_limit = duplicated[:min(
                len(duplicated), self.DISPLAY_DUPL_INDICES_LIMIT
            )]
            raise ValueError(
                f"The '{series.name}' column values are not unique, as"
                " requested.\n"
                f"There are {len(duplicated)} non unique values, and here are"
                " some of them:\n"
                f"{duplicated_limit}..."
            )

        return series
