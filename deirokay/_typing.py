from typing import Any, Callable, Dict, List, TypeVar, Union

import dask.dataframe  # lazy module
import pandas  # lazy module
from typing_extensions import Literal, get_args

try:
    from typing_extensions import Protocol
except ModuleNotFoundError:
    # Protocol exists only since Python 3.8
    Protocol = object

from .enums import Backend

DeirokayDataSource = TypeVar(
    "DeirokayDataSource", "pandas.DataFrame", "dask.dataframe.DataFrame"
)
DeirokayDataSeries = TypeVar(
    "DeirokayDataSeries", "pandas.Series", "dask.dataframe.Series"
)


# docstr-coverage:excused `No need for _typing module`
class DeirokayReadCallable(Protocol):  # noqa: E302 # docstr-coverage comment
    def __call__(
        self,
        data: Union[str, DeirokayDataSource],
        columns: List[str],
        *args: Any,
        **kwargs: Any
    ) -> DeirokayDataSource:
        ...


# TODO: assert if all backends are listed in TypeVars above


BackendValue = Literal["pandas", "dask"]
assert set(get_args(BackendValue)) == {backend.value for backend in Backend}

DeirokayOption = Dict[str, Any]
DeirokayColumnOptions = Dict[str, DeirokayOption]
DeirokayOptionsDocument = Dict[str, Union[str, DeirokayColumnOptions]]

DeirokayStatement = Dict[str, Any]
DeirokayValidationItem = Dict[str, Union[str, List[DeirokayStatement]]]
DeirokayValidationDocument = Dict[str, Union[str, List[DeirokayValidationItem]]]
DeirokayValidationResultDocument = Dict

DeirokaySerializedSeries = Dict[Literal["values", "parser"], Union[List, Dict]]

AnyClass = TypeVar("AnyClass", bound=type)
AnyCallable = Callable[..., Any]
GeneralDecorator = Callable[[AnyCallable], AnyCallable]
