"""Define statement methods that can be aliased in function of the
backend in use."""

from deirokay._typing import GeneralDecorator
from deirokay.backend import register_backend_method
from deirokay.enums import Backend


def report(backend: Backend, *, force: bool = False) -> GeneralDecorator:
    """Define a decorator that turns any method into an alias for the
    `report` method when executed using a given `backend`.

    Parameters
    ----------
    backend : Backend
        The backend for the decorated method.

    Returns
    -------
    GeneralDecorator
        A decorator for the alias method of `report`.
    force : bool, optional
        Force overwrite target method when it already exists.
        Defaults to False.
    """
    return register_backend_method("report", backend, force=force)


def profile(backend: Backend, *, force: bool = False) -> GeneralDecorator:
    """Define a decorator that turns any method into an alias for the
    `profile` method when executed using a given `backend`.

    Parameters
    ----------
    backend : Backend
        The backend for the decorated method.

    Returns
    -------
    GeneralDecorator
        A decorator for the alias method of `profile`.
    force : bool, optional
        Force overwrite target method when it already exists.
        Defaults to False.
    """
    return register_backend_method("profile", backend, force=force)
