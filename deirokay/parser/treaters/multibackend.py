from deirokay._typing import GeneralDecorator
from deirokay.backend import register_backend_method
from deirokay.enums import Backend


def treat(backend: Backend) -> GeneralDecorator:
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
    """
    return register_backend_method('treat', backend)


def serialize(backend: Backend) -> GeneralDecorator:
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
    """
    return register_backend_method('serialize', backend)
