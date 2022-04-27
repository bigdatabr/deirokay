from abc import ABC
from typing import Any, List, Optional

from deirokay._typing import DeirokayDataSeries, DeirokaySerializedSeries
from deirokay.backend import MultiBackendMixin
from deirokay.enums import DTypes


class BaseTreater(MultiBackendMixin, ABC):
    """Base class for all data treaters."""
    supported_backends = []
    supported_dtype: Optional[DTypes] = None
    """Optional[DTypes]: DType treated by this treater."""
    supported_primitives: List[Any] = []
    """List[Any]: Primitive supported by this treater."""

    def __call__(self, series: DeirokayDataSeries, /) -> DeirokayDataSeries:
        """Proxy for `treat`."""
        return self.treat(series)

    def treat(self, series: DeirokayDataSeries, /) -> DeirokayDataSeries:
        """Treat a raw Series to match data expectations for parsing
        and formatting.

        Calls the appropriate method for the detected backend

        Parameters
        ----------
        series : Iterable
            Raw series to be treated.
        """
        raise NotImplementedError

    @staticmethod
    def serialize(series: DeirokayDataSeries,
                  /) -> DeirokaySerializedSeries:
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
        series : DeirokayDataSeries
            Series data to be serialized.

        Returns
        -------
        dict
            A Python dict containing the keys `values` and `parser`.
        """
        raise NotImplementedError
