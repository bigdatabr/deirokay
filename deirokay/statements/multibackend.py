"""Define statement methods that can be aliased in function of the
backend in use."""

from deirokay._typing import GeneralDecorator
from deirokay.backend import register_backend_method
from deirokay.enums import Backend


def report(backend: Backend) -> GeneralDecorator:
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
    """
    return register_backend_method('report', backend)


def profile(backend: Backend) -> GeneralDecorator:
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
    """
    return register_backend_method('profile', backend)
