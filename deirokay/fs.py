"""
Module for FileSystem abstractions and utilities for multi-purpose
paths.
"""

import contextlib
import importlib
import itertools
import json
import os
import re
import sys
from collections import deque
from os.path import splitext
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import ModuleType
from typing import IO, Generator, Iterable, Literal, Optional, Sequence

import yaml

boto3_import_error = None
try:
    import boto3
except ImportError as e:
    boto3 = None
    boto3_import_error = e


BUCKET_KEY_REGEX = re.compile(r's3:\/\/([\w\-]+)\/([\w\-\/.]*)')


def split_s3_path(s3_path: str) -> tuple:
    """Split a full s3 path into `bucket` and `key` parts.

    Parameters
    ----------
    s3_path : str
        Full S3 path, such as `s3://my-bucket/my-prefix/` or
        `s3://my-bucket/my-prefix/my-file.txt`.

    Returns
    -------
    tuple
        `(bucket, key)` tuple.
    """
    bucket, key = BUCKET_KEY_REGEX.findall(s3_path)[0]
    return bucket, key


def _import_file_as_python_module(path_to_file: str) -> ModuleType:
    """Import a .py file as a Python module.

    Parameters
    ----------
    path_to_file : str
        Path to .py file.

    Returns
    -------
    ModuleType
        Imported module.

    Raises
    ------
    ValueError
        Not a valid Python file.
    """

    module_path, extension = os.path.splitext(path_to_file)
    module_name = os.path.basename(module_path)

    if extension not in ('.py', '.o'):
        raise ValueError('You should pass a valid Python file')

    module_dir = os.path.dirname(path_to_file)
    sys.path.insert(0, module_dir)
    module = importlib.import_module(module_name)
    sys.path.pop(0)

    return module


class FileSystem():
    """Abstract file system operations over folders and files in
    any place, such as local or in S3 buckets.

    Parameters
    ----------
    path : str
        Path to file or folder.
    """

    def __init__(self, path: str):
        self.path = path

    def ls(self, recursive: bool = False, files_only: bool = False,
           reverse: bool = False, limit: Optional[int] = None
           ) -> Sequence['FileSystem']:
        """List files in a prefix or folder.

        Parameters
        ----------
        recursive : bool, optional
            Whether or not to list subfolders recursively,
            by default False
        files_only : bool, optional
            List only files, ignore folders/prefixes,
            by default False.
        """
        raise NotImplementedError

    def read_dict(self, *args, **kwargs) -> dict:
        """Read and parse a `dict`-like file from either YAML or JSON
        format.

        Returns
        -------
        dict
            Python dictionary with the file content.
        """
        extension = splitext(self.path)[1].lower()
        if extension == '.json':
            return self.read_json(*args, **kwargs)
        elif extension in ('.yaml', '.yml'):
            return self.read_yaml(*args, **kwargs)
        raise NotImplementedError(f'No parser for file type: {extension}')

    def write_dict(self, *args, **kwargs) -> None:
        """Serialize and write a Python `dict` to either YAML or JSON
        file."""
        extension = splitext(self.path)[1].lower()
        if extension == '.json':
            return self.write_json(*args, **kwargs)
        elif extension in ('.yaml', '.yml'):
            return self.write_yaml(*args, **kwargs)
        raise NotImplementedError(f'No serializer for file type: {extension}')

    def read_yaml(self) -> dict:
        """Read and parse a YAML file as a Python `dict`.

        Returns
        -------
        dict
            Python dictionary with the file content.
        """
        with self.open('r') as fp:
            return yaml.safe_load(fp)

    def write_yaml(self, doc: dict, **kwargs) -> None:
        """Serialize and write a Python `dict` to a YAML file.

        Parameters
        ----------
        doc : dict
            The Python `dict`.

        """
        with self.open('w') as fp:
            yaml.dump(doc, fp, sort_keys=False, **kwargs)

    def read_json(self) -> dict:
        """Read and parse a JSON file as a Python `dict`.

        Returns
        -------
        dict
            Python dictionary with the file content.
        """
        with self.open('r') as fp:
            return json.load(fp)

    def write_json(self, doc: dict, **kwargs) -> None:
        """Serialize and write a Python `dict` to a JSON file.

        Parameters
        ----------
        doc : dict
            The Python `dict`.
        """
        with self.open('w') as fp:
            json.dump(doc, fp, **kwargs)

    def isdir(self) -> bool:
        """Return True if the path is a directory.

        Returns
        -------
        bool
            Whether or not the path is a directory

        Raises
        ------
        NotImplementedError
            Operation not valid or not implemented.
        """
        raise NotImplementedError

    def mkdir(self, *args, **kwargs):
        """Create directory using `Path.mkdir` method. Arguments are
        passed directly to this method.

        Raises
        ------
        NotImplementedError
            Operator not valid or not implemented.
        """
        raise NotImplementedError

    def import_as_python_module(self) -> ModuleType:
        """Import file as a Python module."""
        raise NotImplementedError

    def open(self, mode: Literal['r', 'w'], *args, **kwargs) -> IO:
        """Open file."""
        raise NotImplementedError

    def read(self, *args, **kwargs) -> str:
        """Read a file as text."""
        with self.open(*args, mode='r', **kwargs) as fp:
            return fp.read()

    def __truediv__(self, rest: str) -> 'FileSystem':
        """Create another FileSystem object by '/'-joining a FileSystem
        object with a string.

        Parameters
        ----------
        rest : str
            The rest of the file path.

        Returns
        -------
        FileSystem
            The same FileSystem subclass as the original object.

        Raises
        ------
        TypeError
            `rest` should be a `str`.
        """
        if isinstance(rest, str):
            cls = type(self)
            return cls(os.path.join(self.path, rest))
        raise TypeError()

    def __lt__(self, other: 'FileSystem') -> bool:
        assert isinstance(self, type(other))
        return self.path.__lt__(other.path)

    def __str__(self) -> str:
        return self.path


class LocalFileSystem(FileSystem):
    """FileSystem wrapper for local files and folders."""

    # docstr-coverage:inherited
    def ls(self, recursive: bool = False, files_only: bool = False,
           reverse: bool = False, limit: Optional[int] = None
           ) -> Sequence['LocalFileSystem']:
        if recursive is False:
            raise NotImplementedError
        if files_only is False:
            raise NotImplementedError

        def _recursive_list():
            for parent, _, files in sorted(os.walk(self.path),
                                           key=lambda parent, *_: parent,
                                           reverse=reverse):
                for file in sorted(files, reverse=reverse):
                    yield os.path.join(parent, file)

        limited_files = itertools.islice(_recursive_list(), limit)

        return [LocalFileSystem(path) for path in limited_files]

    # docstr-coverage:inherited
    def import_as_python_module(self) -> ModuleType:
        module = _import_file_as_python_module(self.path)
        return module

    # docstr-coverage:inherited
    def isdir(self) -> bool:
        return os.path.isdir(self.path)

    # docstr-coverage:inherited
    def mkdir(self, *args, **kwargs) -> None:
        return Path(self.path).mkdir(*args, **kwargs)

    # docstr-coverage:inherited
    def open(self, mode: Literal['r', 'w'], *args, **kwargs) -> IO:
        return open(self.path, mode, *args, **kwargs)


class S3FileSystem(FileSystem):
    """FileSystem wrapper for objects stored in AWS S3."""

    LIST_OBJECTS_MAX_KEYS = 1000

    def __init__(self, path: Optional[str] = None,
                 bucket: Optional[str] = None,
                 prefix_or_key: Optional[str] = None,
                 client: Optional['boto3.client'] = None):

        if boto3 is None:
            raise ImportError('S3-backend requires `boto3` module to be'
                              f' installed ({boto3_import_error})')

        if bool(path) == bool(bucket) or bool(path) == bool(prefix_or_key):
            raise ValueError(
                'Either `path` or `(bucket, prefix_or_key)` should be'
                ' passed (but not both).'
            )

        if bucket and prefix_or_key:
            final_bucket, final_prefix_or_key = bucket, prefix_or_key
            final_path = os.path.join(bucket, prefix_or_key)
        elif path:
            final_bucket, final_prefix_or_key = split_s3_path(path)
            final_path = path

        super().__init__(final_path)
        self.client = client or boto3.client('s3')
        self.bucket = final_bucket
        self.prefix_or_key = final_prefix_or_key

    # docstr-coverage:inherited
    def ls(self, recursive: bool = False, files_only: bool = False,
           reverse: bool = False, limit: Optional[int] = None
           ) -> Sequence['S3FileSystem']:
        if recursive is False:
            raise NotImplementedError
        if files_only is False:
            raise NotImplementedError

        # To handle requests containing over 1000 items, we need to
        # paginate through the results.
        max_keys = self.LIST_OBJECTS_MAX_KEYS
        page_iterable = self.client.get_paginator('list_objects_v2').paginate(
            Bucket=self.bucket,
            Prefix=self.prefix_or_key,
            PaginationConfig={'PageSize': max_keys}
        )
        chained_items: Iterable
        if not reverse:
            chained_items = (
                item['Key']
                for page in page_iterable
                for item in page.get('Contents', [])
            )
        else:
            # Use deque with maxlen to exhaust the paginator and keep
            # only the last pages (those necessary to satisfy the
            # limit).
            pages_to_keep = (
                None if limit is None else (limit+max_keys-2)//max_keys+1
            )
            last_pages = deque(iter(page_iterable), maxlen=pages_to_keep)
            # Worst case scenario: the last page contains only one
            # item. Hence, from 2 to 1001, we need at least 2 pages,
            # and so on. Thus, the expression `(limit+998)//1000 + 1`
            chained_items = (
                item['Key']
                for page in reversed(last_pages)
                for item in reversed(page.get('Contents', []))
            )

        chained_items = itertools.islice(chained_items, limit)
        return [
            S3FileSystem(bucket=self.bucket,
                         prefix_or_key=key,
                         client=self.client)
            for key in chained_items
        ]

    # docstr-coverage:inherited
    def import_as_python_module(self) -> ModuleType:
        _, extension = os.path.splitext(self.path)
        with NamedTemporaryFile(suffix=extension) as tmp_fp:
            self.client.download_file(self.bucket,
                                      self.prefix_or_key,
                                      tmp_fp.name)
            module = _import_file_as_python_module(tmp_fp.name)
            return module

    # docstr-coverage:inherited
    def open(self, mode: Literal['r', 'w'], *args, **kwargs) -> IO:
        if mode == 'r':
            body = self.client.get_object(Bucket=self.bucket,
                                          Key=self.prefix_or_key)['Body']
            return contextlib.closing(body)  # type: ignore
        elif mode == 'w':
            def _writable() -> Generator[IO, None, None]:
                with NamedTemporaryFile(mode, *args, **kwargs) as tmp_fp:
                    yield tmp_fp
                    tmp_fp.flush()
                    self.client.upload_file(
                        tmp_fp.name,
                        self.bucket,
                        self.prefix_or_key
                    )
            return contextlib.contextmanager(_writable)()  # type: ignore
        raise NotImplementedError(f"Mode '{mode}' not supported.")


def fs_factory(path: str) -> FileSystem:
    """Factory for FileSystem objects.

    Parameters
    ----------
    path : str
        Local or S3 path.

    Returns
    -------
    FileSystem
        Proper FileSystem object for the path provided.
    """
    if path.startswith('s3://'):
        return S3FileSystem(path)
    else:
        return LocalFileSystem(path)
