from typing import Any, Callable, Dict, List, Literal, TypeVar, Union, get_args

import dask.dataframe  # lazy module
import pandas  # lazy module
from typing_extensions import Protocol

from .enums import Backend

DeirokayDataSource = TypeVar('DeirokayDataSource',
                             'pandas.DataFrame', 'dask.dataframe.DataFrame')
DeirokayDataSeries = TypeVar('DeirokayDataSeries',
                             'pandas.Series', 'dask.dataframe.Series')


# docstr-coverage:excused `No need for _typing module`
class DeirokayReadCallable(Protocol):  # noqa: E302 # docstr-coverage comment
    def __call__(self, data: Union[str, DeirokayDataSource],
                 columns: List[str],
                 *args: Any,
                 **kwargs: Any) -> DeirokayDataSource: ...

# TODO: assert if all backends are listed in TypeVars above


BackendValue = Literal['pandas', 'dask']
assert set(get_args(BackendValue)) == {backend.value for backend in Backend}

DeirokayOption = Dict[str, Any]
DeirokayColumnOptions = Dict[str, DeirokayOption]
DeirokayOptionsDocument = Dict[str, Union[str, DeirokayColumnOptions]]

DeirokayStatement = Dict[str, Any]
DeirokayValidationItem = Dict[str, Union[str, List[DeirokayStatement]]]
DeirokayValidationDocument = Dict[str,
                                  Union[str, List[DeirokayValidationItem]]]
DeirokayValidationResultDocument = Dict

DeirokaySerializedSeries = Dict[Literal['values', 'parser'], Union[List, Dict]]

AnyClass = TypeVar('AnyClass', bound=type)
AnyCallable = Callable[..., Any]
GeneralDecorator = Callable[[AnyCallable], AnyCallable]
