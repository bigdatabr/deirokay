from deirokay._typing import GeneralDecorator
from deirokay.backend import register_backend_method
from deirokay.enums import Backend


def treat(backend: Backend, *, force: bool = False) -> GeneralDecorator:
    """Define a decorator that turns any method into an alias for the
    `treat` method when executed using a given `backend`.

    Parameters
    ----------
    backend : Backend
        The backend for the decorated method.

    Returns
    -------
    GeneralDecorator
        A decorator for the alias method of `treat`.
    force : bool, optional
        Force overwrite target method when it already exists.
        Defaults to False.
    """
    return register_backend_method('treat', backend, force=force)


def serialize(backend: Backend, *, force: bool = False) -> GeneralDecorator:
    """Define a decorator that turns any method into an alias for the
    `serialize` method when executed using a given `backend`.

    Parameters
    ----------
    backend : Backend
        The backend for the decorated method.

    Returns
    -------
    GeneralDecorator
        A decorator for the alias method of `serialize`.
    force : bool, optional
        Force overwrite target method when it already exists.
        Defaults to False.
    """
    return register_backend_method('serialize', backend, force=force)
