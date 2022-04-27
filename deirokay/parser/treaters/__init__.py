from typing import Any, Type

from deirokay._typing import DeirokayOption
from deirokay._utils import recursive_subclass_generator
from deirokay.backend import multibackend_class_factory
from deirokay.enums import Backend, DTypes

from .builtin import BaseTreater

DTYPE_2_TREATER = {
    cls.supported_dtype: cls
    for cls in recursive_subclass_generator(BaseTreater)
    if cls.supported_dtype is not None
}

PRIMITIVE_2_TREATER = {
    primitive: cls
    for cls in recursive_subclass_generator(BaseTreater)
    for primitive in cls.supported_primitives
}


def get_dtype_treater(dtype: Any) -> Type[BaseTreater]:
    """Map a dtype to its Treater class."""
    try:
        if isinstance(dtype, DTypes):
            return DTYPE_2_TREATER[dtype]
        elif isinstance(dtype, str):
            return DTYPE_2_TREATER[DTypes(dtype)]
        else:
            return PRIMITIVE_2_TREATER[dtype]

    except KeyError as e:
        raise NotImplementedError(f"Handler for '{dtype}' hasn't been"
                                  " implemented yet") from e


def get_treater_instance(option: DeirokayOption,
                         backend: Backend) -> BaseTreater:
    """Create a treater instance from a Deirokay-style option.

    Example
    -------

    .. code-block:: python

        option = {
            'dtype': 'integer',
            'thousand_sep': ','
        }
    """
    option = option.copy()
    dtype = option.pop('dtype')

    cls = get_dtype_treater(dtype)
    return multibackend_class_factory(cls, backend)(**option)
