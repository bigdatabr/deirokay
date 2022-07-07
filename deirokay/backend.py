import functools
import warnings
from typing import Callable, Dict, List, Optional, Type, TypeVar, Union

from deirokay.exceptions import InvalidBackend, UnsupportedBackend

from .__version__ import __comp_version__
from ._typing import AnyCallable, DeirokayDataSource
from .enums import Backend

MODULE_2_BACKEND = {
    'pandas': Backend.PANDAS,
    'dask.dataframe': Backend.DASK,
}
assert sorted(Backend) == sorted(MODULE_2_BACKEND.values())

BACKEND_2_MODULE = {v: k for k, v in MODULE_2_BACKEND.items()}


_AnyMultiBackendClass = TypeVar('_AnyMultiBackendClass',
                                bound='MultiBackendMixin')


class MultiBackendMixin:
    """Mixin class which all multi-backend resources in Deirokay
    derives from.

    Together with the `register_backend_method` decorator, the methods
    of its subclasses can be marked to be active when a particular
    backend is active.
    """
    supported_backends: List[Backend] = []
    """List[Backend]: Backends supported by this resource."""
    _current_backend: Optional[Backend] = None
    """Optional[Backend]: Current active backend
    (only valid during runtime)."""
    _backend_methods: Dict[Backend, Dict[str, AnyCallable]] = {}

    @classmethod
    def register_backend_method(cls,
                                alias_for: str,
                                func: AnyCallable,
                                backend: Backend) -> None:
        """Proxy for `register_backend_method` to register an existing
        function as a backend-specific method.

        Parameters
        ----------
        alias_for : str
            The name of the method to be substituted with a
            backend-specific version.
        func : AnyCallable
            Existing function to be registered as a method.
        backend : Backend
            Backend for the method.
        """
        backend_method = register_backend_method(alias_for, backend)(func)
        method_name = f'_registered_{alias_for}_{backend.value}'
        setattr(cls, method_name, backend_method)
        # When the decorator is not called from inside a class scope,
        # the __set_name__ hook should be invoked manually.
        # See: https://docs.python.org/3/reference/datamodel.html?highlight=__set_name__#object.__set_name__  # noqa: E501
        backend_method.__set_name__(cls, method_name)  # type: ignore  # The decorator replaces itself in the owner class with the original method  # noqa: E501

    @classmethod
    @functools.lru_cache(maxsize=None)
    def attach_backend(cls: Type['MultiBackendMixin'],
                       backend: Backend
                       ) -> Type['_AnyMultiBackendClass']:
        """Generate a subclass that concretizes multibackend backend
        methods into their intended name. The methods marked with the
        given `backend` will compose the returned class.

        Parameters
        ----------
        cls : type
            Class to be subclassed with the given backend.
        backend : Backend
            Backend to be selected.

        Returns
        -------
        Type[MultiBackendMixin]
            Subclass of the current class with methods filtered for the
            given backend.
        """
        if (
            __comp_version__ < (2,)
            and cls.supported_backends == []
            and 'supported_backends' not in vars(cls)
        ):
            warnings.warn(
                'To preserve backward compatibility, the'
                f' `supported_backends` attribute from `{cls.__name__}`'
                ' is assumed to be `[Backend.PANDAS]` when not declared.'
                ' In future, this behavior will change and an exception'
                ' will be raised whenever a multibackend class does not'
                ' specify this attribute explicitely.\n'
                'To prevent this error in future and suppress this'
                ' warning in the current version, please set the'
                ' `supported_backends` class attribute explicitely.',
                FutureWarning
            )
            cls.supported_backends = [Backend.PANDAS]

        if backend not in cls.supported_backends:
            raise UnsupportedBackend(
                f"The `{cls.__name__}` class does not support the '{backend}'"
                f' backend, only the following ones: {cls.supported_backends}.'
                f' If you are using a custom class, be sure to provide a'
                f' `supported_backends` attribute containing {backend!s}.'
            )

        execution_name = f'{cls.__name__}-{backend.value.capitalize()}Backend'
        execution_attrs = dict(vars(cls))
        execution_subclass = type(
            execution_name,
            (cls,),
            execution_attrs
        )  # type: Type[_AnyMultiBackendClass]

        _merged_backend_methods = {}
        for _cls in reversed(cls.mro()):
            if not issubclass(_cls, MultiBackendMixin):
                continue
            try:
                _merged_backend_methods.update(_cls._backend_methods[backend])
            except KeyError:
                continue

        for alias_for, (method, force) in _merged_backend_methods.items():
            if not force and alias_for in vars(execution_subclass):
                raise InvalidBackend(
                    f'You cannot declare the alias method `{method.__name__}`'
                    f' in the class `{cls}` when the original method'
                    f' `{alias_for}` already exists or has already been'
                    ' replaced by another alias.'
                    f' Either remove `{alias_for}` or make sure only'
                    f' one alias of `{alias_for}` exists for the'
                    f" '{backend.value}' backend to continue.\n"
                    "Alternatively, you may set the flag `force=True`"
                    " when calling"
                )
            setattr(execution_subclass, alias_for, method)

        # Save backend for future reference (via `get_backend`)
        execution_subclass._current_backend = backend
        # Call overwritable user method
        execution_subclass.__post_attach_backend__()
        return execution_subclass

    @classmethod
    def __post_attach_backend__(cls):
        """This classmethod can be optionally overwritten to serve as a
        callback function for when the `attach_backend()` method is
        called."""

    @classmethod
    def get_backend(cls) -> Backend:
        """Get current active backend for this class.

        Returns
        -------
        Backend
            The current active backend.

        Raises
        ------
        InvalidBackend
            Backend not set or not a valid execution class.
        """
        if cls._current_backend is None:
            raise InvalidBackend('Backend not set for current execution')
        return cls._current_backend


DecoratorClass = Union[Callable, Type]


def register_backend_method(
    alias_for: str,
    backend: Backend,
    *,
    force: bool = False
) -> DecoratorClass:
    """Modify a method to make it an alternative (alias) for another
    method (`alias_for`) when the specified backend is active.
    Should be used as an decorator as in:
    `@register_backend_method('method_name', <Backend object>)`.

    It could be handful to declare helper decorators using this one for
    common methods for a given resource. For instance, if a hierarchy
    of classes should implement a `do_it` method to work with different
    backend, we could implement a `do_it` decorator and use it to
    decorate backend-specific methods.

    .. code-block:: python

        def do_it(backend):
            register_backend_method('do_it', backend)

        class MultiBackendClass(MultiBackendMixin):
            @do_it(Backend.TYPE1)
            def _do_it_1(...):
                ...

            @do_it(Backend.TYPE2)
            def _do_it_2(...):
                ...

    Parameters
    ----------
    alias_for : str
        The name of the method to be substituted with a
        backend-specific version.
    backend : Backend
        The backend to use when running the method.
    force : bool, optional
        Force overwrite target method when it already exists.
        Defaults to False.
    """
    assert isinstance(backend, Backend), (
        'Make sure this decorator declares a `backend` argument'
    )

    class _decorator():
        def __init__(self, decorated_method: AnyCallable) -> None:
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
            owner._backend_methods[backend][alias_for] = (method, force)

    return _decorator


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
