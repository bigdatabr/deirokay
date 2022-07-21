from difflib import get_close_matches
from typing import Generator, Iterable, TypeVar, Union

from jinja2.nativetypes import Environment

C = TypeVar('C', bound=type)


def recursive_subclass_generator(cls: C) -> Generator[C, None, None]:
    """Iterate over all subclasses of `cls` recursively (including
    itself).

    Parameters
    ----------
    cls : C
        Class to iterate over.

    Yields
    ------
    Generator[C, None, None]
        Each of its subclasses, recursively.
    """
    yield cls
    for subclass in cls.__subclasses__():
        yield from recursive_subclass_generator(subclass)


def check_columns_in_df_columns(columns: Iterable,
                                df_columns: Iterable) -> None:
    """Raise an exception if any of the elements from `columns` is not
    present in `df_columns`.

    Parameters
    ----------
    columns : Iterable
        List of requested elements.
    df_columns : Iterable
        List of available elements.

    Raises
    ------
    KeyError
        Element not found.
    """
    miss = {}
    for col in columns:
        if col not in df_columns:
            match = get_close_matches(col, df_columns, n=1, cutoff=0)[0]
            miss[col] = match
    if miss:
        raise KeyError(f'Columns {list(miss.keys())} not found in your data.'
                       f' Did you mean {list(miss.values())}?')


def render_list(env: Environment, list_: list, template: dict):
    """Render Jinja templates in list recursively."""
    for index, value in enumerate(list_):
        if isinstance(value, str) and '{{' in value:
            rendered = env.from_string(value).render(**template)
            list_[index] = rendered
        elif isinstance(value, dict):
            render_dict(env, value, template)
        elif isinstance(value, list):
            render_list(env, value, template)


def render_dict(env: Environment, dict_: dict, template: dict):
    """Render Jinja templates in dict recursively.

    It will render only strings starting with `{{` to prevent
    unattended renderings."""
    for key, value in dict_.items():
        if isinstance(value, str) and '{{' in value:
            rendered = env.from_string(value).render(**template)
            dict_[key] = rendered
        elif isinstance(value, dict):
            render_dict(env, value, template)
        elif isinstance(value, list):
            render_list(env, value, template)


T = TypeVar('T')


def noneor(*operands: T) -> Union[T, None]:
    """Return the first non-None element."""
    for item in operands:
        if item is not None:
            return item
    return None
