"""
Set of functions and classes related to gathering of past validation
logs from a given directory and assembling them as pandas Series.
"""

import warnings
from typing import Callable, List

import jq
import pandas  # lazy module

from ._typing import DeirokayValidationDocument
from .fs import FileSystem


def series_from_fs(
    series_name: str, lookback: int, folder: FileSystem
) -> List[DeirokayValidationDocument]:
    """List log files as FileSystem objects for a given Validation
    Document.

    Parameters
    ----------
    series_name : str
        Validation Document name.
    lookback : int
        How many logs to look behind.
    folder : FileSystem
        Where logs are read from.

    Returns
    -------
    List[FileSystem]
        List of logs to be queried.
    """

    ls = (folder / f"{series_name}/").ls(
        recursive=True, files_only=True, reverse=True, limit=lookback
    )
    return [file.read_dict() for file in ls]


class NullCallableNode:
    """Dummy node which returns nothing."""

    def __getattr__(self, name: str) -> Callable[[], None]:
        return lambda: None


class StatementNode:
    """Node at Statement level."""

    detail_keys = jq.compile(".[].report.detail | keys")

    def __init__(self, statements) -> None:
        attributes = StatementNode.detail_keys.input(statements).all()
        attributes = set([key for sub in attributes for key in sub])

        for att in attributes:
            child = jq.compile(f'.[].report.detail["{att}"]').input(statements).all()
            setattr(self, att, pandas.Series(child))

    def __getattr__(self, name: str) -> NullCallableNode:
        return NullCallableNode()


class ItemNode:
    """Node at Item level."""

    attribute_keys = jq.compile(
        ".[].statements[] | if .alias != null then .alias else .type end"
    )

    def __init__(self, items: List[str]):
        attributes = set(ItemNode.attribute_keys.input(items).all())

        for att in attributes:
            child = (
                jq.compile(
                    ".[].statements[] | "
                    f'select(.alias == "{att}" or .type == "{att}")'
                )
                .input(items)
                .all()
            )
            setattr(self, att, StatementNode(child))

    def __getattr__(self, name: str) -> StatementNode:
        return StatementNode([])


class DocumentNode:
    """Node at Document root level."""

    attribute_keys = jq.compile(
        ".[].items[] | if .alias != null then .alias else .scope end"
    )

    def __init__(self, docs: List[dict]):
        try:
            attributes = set(DocumentNode.attribute_keys.input(docs).all())
        except TypeError:
            raise TypeError(
                "All list-like scopes must be aliased"
                " when some statement uses the `series` template."
                " Make sure your last Deirokay logs obey this rule."
            )

        for att in attributes:
            child = (
                jq.compile(
                    ".[].items[] | " f'select(.alias == "{att}" or .scope == "{att}")'
                )
                .input(docs)
                .all()
            )
            setattr(self, att, ItemNode(child))

    def __getattr__(self, name: str):
        return ItemNode([])


def get_series(series_name: str, lookback: int, read_from: FileSystem) -> DocumentNode:
    """Construct a traversable object tree based on past validation
    logs. This method is aliased as `series` when called from inside
    a Validation Document.

    Parameters
    ----------
    series_name : str
        The Validation Document name.
    lookback : int
        How many logs to look behind.
    read_from : FileSystem
        Base folder where logs are read from.

    Returns
    -------
    DocumentNode
        Root node for object tree.

    Raises
    ------
    ValueError
        `read_from` can not be None.
    """
    if read_from is None:
        raise ValueError(
            "You cannot access previous logs without providing"
            " a source/destination directory."
        )
    docs = series_from_fs(series_name, lookback, read_from)

    if not docs:
        warnings.warn(
            "No previous log has been found in the specified folder."
            " Make sure you are reading and writing to the right"
            " place.",
            Warning,
        )

    return DocumentNode(docs)
