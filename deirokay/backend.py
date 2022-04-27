import functools
from typing import Dict, List, TypeVar

from deirokay.exceptions import InvalidBackend, UnsupportedBackend

from ._typing import AnyCallable, DeirokayDataSource, GeneralDecorator
from .enums import Backend

MODULE_2_BACKEND = {
    'pandas': Backend.PANDAS,
    'dask.dataframe': Backend.DASK,
}
assert sorted(Backend) == sorted(MODULE_2_BACKEND.values())

BACKEND_2_MODULE = {v: k for k, v in MODULE_2_BACKEND.items()}


class MultiBackendMixin:
    supported_backends: List[Backend] = []
    """List[Backend]: Backends supported by this resource."""
    _backend_methods: Dict[Backend, Dict[str, AnyCallable]] = {}

    @classmethod
    def register_backend_method(cls,
                                alias_for: str,
                                func: AnyCallable,
                                backend: Backend) -> None:
        backend_method = register_backend_method(alias_for, backend)(func)
        method_name = f'_registered_{alias_for}_{backend.value}'
        setattr(cls, method_name, backend_method)
        # When the decorator is not called from inside a class scope,
        # the __set_name__ hook should be invoked manually.
        # See: https://docs.python.org/3/reference/datamodel.html?highlight=__set_name__#object.__set_name__  # noqa: E501
        backend_method.__set_name__(cls, method_name)


_AnyMultiBackendClass = TypeVar(
    '_AnyMultiBackendClass', bound=MultiBackendMixin
)


def register_backend_method(
    alias_for: str,
    /,
    backend: Backend
) -> GeneralDecorator:
    """Modify a method to make it an alternative (alias) for another
    method (`alias_for`) when the specified backend is active.
    Should be used as an decorator as in:
    `@register_backend_method('method_name', <Backend object>)`.

    Parameters
    ----------
    alias_for : str
        The name of the method to be substituted with a
        backend-specific version.
    backend : Backend
        The backend to use when running the method.
    """
    assert isinstance(backend, Backend), (
        'Make sure this decorator declares a `backend` argument'
    )

    class _decorator():
        def __init__(self, decorated_method: AnyCallable, /) -> None:
            self.decorated_method = decorated_method

        def __set_name__(self, owner: type, method_name: str) -> None:
            setattr(owner, method_name, self.decorated_method)
            if not issubclass(owner, MultiBackendMixin):
                raise InvalidBackend(
                    f'Make sure the `{owner.__name__}` class subclasses'
                    f' `{MultiBackendMixin.__name__}` before using'
                    f' `{register_backend_method.__name__}`'
                )
            if backend not in owner.supported_backends:
                raise UnsupportedBackend(
                    f'The `{owner.__name__}` class does not seem to support'
                    f" the '{backend.value}' backend used in `{method_name}`."
                    ' Make sure to explicitely declare a `supported_backends`'
                    f" attribute containing the '{backend.value}' backend."
                )
            if '_backend_methods' not in vars(owner):
                # We must reinitialize _backend_methods in all subclasses to
                # prevent all of them sharing the same
                # `MultiBackendMixin._backend_methods` attribute
                owner._backend_methods = {}
            if backend not in owner._backend_methods:
                owner._backend_methods[backend] = {}
            # We use `vars()` to get method since `getattr()` would resolve
            # existing descriptors (notably, staticmethod and classmethod)
            method = vars(owner)[method_name]
            owner._backend_methods[backend][alias_for] = method

    return _decorator


@functools.cache
def multibackend_class_factory(cls: _AnyMultiBackendClass,
                               backend: Backend) -> _AnyMultiBackendClass:
    """Create an execution subclass for use with the specified backend.
    The methods marked with the given `backend` will compose the
    returned class.

    Parameters
    ----------
    cls : type
        Class to be subclassed with the given backend.
    backend : Backend
        Backend to filter marked methods.

    Returns
    -------
    type
        Subclass with methods filtered for the given backend.
    """
    if not issubclass(cls, MultiBackendMixin):
        raise InvalidBackend(
            f'`{cls.__name__}` should be a subclass of `{MultiBackendMixin}`'
        )

    if backend not in cls.supported_backends:
        raise UnsupportedBackend(
            f"The `{cls.__name__}` class does not support the '{backend}'"
            f' backend, only the following ones: {cls.supported_backends}.'
            f' If you are using a custom class, be sure to provide a'
            f' `supported_backends` attribute.'
        )

    execution_name = f'{cls.__name__}-{backend.value.capitalize()}Backend'
    execution_attrs = dict(vars(cls))
    execution_subclass = type(execution_name, (cls,), execution_attrs)

    _merged_backend_methods = {}
    for _cls in reversed(cls.mro()):
        if not issubclass(_cls, MultiBackendMixin):
            continue
        try:
            _merged_backend_methods.update(_cls._backend_methods[backend])
        except KeyError:
            continue

    for alias_for, method in _merged_backend_methods.items():
        if alias_for in vars(execution_subclass):
            raise InvalidBackend(
                f'You cannot declare the alias method `{method.__name__}`'
                f' in the class `{cls}` when the original method'
                f' `{alias_for}` already exists or has already been'
                ' replaced by another alias.'
                f' Either remove `{alias_for}` or make sure only'
                f' one alias of `{alias_for}` exists for the'
                f" '{backend.value}' backend to continue."
            )
        setattr(execution_subclass, alias_for, method)

    return execution_subclass


def _detect_backend_from_type(object_type: type) -> Backend:
    """Infers the backend from the object's module name."""
    for _module in MODULE_2_BACKEND:
        if object_type.__module__.startswith(_module):
            return MODULE_2_BACKEND[_module]
    raise UnsupportedBackend(
        f'Unknown backend for {object_type}.'
        f' Supported backends: {set(i.value for i in Backend)}.'
    )


def detect_backend(df: DeirokayDataSource) -> Backend:
    """Map the object class to the proper backend value.

    Parameters
    ----------
    df : Union[DataFrame, DaskDataFrame]
        DataFrame object.

    Returns
    -------
    Backend
        Instance of `Backend` enumeration.

    Raises
    ------
    ValueError
        Unknown backend for the specified object type.
    """
    return _detect_backend_from_type(type(df))
