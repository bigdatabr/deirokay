from difflib import get_close_matches
from typing import Iterable

from jinja2.nativetypes import Environment


def _check_columns_in_df_columns(columns: Iterable, df_columns: Iterable):
    miss = {}
    for col in columns:
        if col not in df_columns:
            match = get_close_matches(col, df_columns, n=1, cutoff=0)[0]
            miss[col] = match
    if miss:
        raise KeyError(f'Columns {list(miss.keys())} not found in your data.'
                       f' Did you mean {list(miss.values())}?')


def _render_list(env: Environment, list_: list, template: dict):
    """Render Jinja templates in list recursively."""
    for index, value in enumerate(list_):
        if isinstance(value, str) and '{{' in value:
            rendered = env.from_string(value).render(**template)
            list_[index] = rendered
        elif isinstance(value, dict):
            _render_dict(env, value, template)
        elif isinstance(value, list):
            _render_list(env, value, template)


def _render_dict(env: Environment, dict_: dict, template: dict):
    """Render Jinja templates in dict recursively.

    It will render only strings starting with `{{` to prevent
    unattended renderings."""
    for key, value in dict_.items():
        if isinstance(value, str) and '{{' in value:
            rendered = env.from_string(value).render(**template)
            dict_[key] = rendered
        elif isinstance(value, dict):
            _render_dict(env, value, template)
        elif isinstance(value, list):
            _render_list(env, value, template)
