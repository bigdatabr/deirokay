"""
Module for FileSystem abstractions and utilities for multi-purpose
paths.
"""

import importlib
import json
import os
import re
from os.path import splitext
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import ModuleType
from typing import List, Optional

import yaml

boto3_import_error = None
try:
    import boto3
except ImportError as e:
    boto3 = None
    boto3_import_error = e


BUCKET_KEY_REGEX = re.compile(r's3:\/\/([\w\-]+)\/([\w\-\/.]+)')


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
    os.sys.path.insert(0, module_dir)
    module = importlib.import_module(module_name)
    os.sys.path.pop(0)

    return module


class FileSystem():
    """Abstract file system operations over folders and files in
    any place, such as local or in S3 buckets."""

    def __init__(self, path: str):
        """Construct a FileSystem object from a path.

        Parameters
        ----------
        path : str
            Path to file or folder.
        """
        self.path = path

    def ls(self, recursive=False, files_only=False) -> List['FileSystem']:
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

    def write_dict(self, *args, **kwargs):
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
        raise NotImplementedError

    def write_yaml(self, doc: dict, **kwargs):
        """Serialize and write a Python `dict` to a YAML file.

        Parameters
        ----------
        doc : dict
            The Python `dict`.

        """
        raise NotImplementedError

    def read_json(self) -> dict:
        """Read and parse a JSON file as a Python `dict`.

        Returns
        -------
        dict
            Python dictionary with the file content.
        """
        raise NotImplementedError

    def write_json(self, doc: dict, **kwargs):
        """Serialize and write a Python `dict` to a JSON file.

        Parameters
        ----------
        doc : dict
            The Python `dict`.
        """
        raise NotImplementedError

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

    def __lt__(self, other: 'FileSystem'):
        assert isinstance(self, type(other))
        return self.path.__lt__(other.path)

    def __str__(self):
        return self.path


class LocalFileSystem(FileSystem):
    """FileSystem wrapper for local files and folders."""

    # docstr-coverage:inherited
    def ls(self, recursive=False, files_only=False) -> List['LocalFileSystem']:
        if recursive is False:
            raise NotImplementedError

        acc = []
        for parent, folders, files in os.walk(self.path):
            if not files_only:
                acc += [os.path.join(parent, folder) for folder in folders]
            acc += [os.path.join(parent, file) for file in files]
        return [LocalFileSystem(path) for path in acc]

    # docstr-coverage:inherited
    def read_yaml(self) -> dict:
        with open(self.path) as fp:
            return yaml.load(fp, Loader=yaml.CLoader)

    # docstr-coverage:inherited
    def write_yaml(self, doc: dict, **kwargs) -> None:
        with open(self.path, 'w') as fp:
            yaml.dump(doc, fp, sort_keys=False, **kwargs)

    # docstr-coverage:inherited
    def read_json(self) -> dict:
        with open(self.path) as fp:
            return json.load(fp)

    # docstr-coverage:inherited
    def write_json(self, doc: dict, **kwargs) -> None:
        with open(self.path, 'w') as fp:
            json.dump(doc, fp, **kwargs)

    # docstr-coverage:inherited
    def import_as_python_module(self):
        module = _import_file_as_python_module(self.path)
        return module

    # docstr-coverage:inherited
    def isdir(self):
        return os.path.isdir(self.path)

    # docstr-coverage:inherited
    def mkdir(self, *args, **kwargs):
        return Path(self.path).mkdir(*args, **kwargs)


class S3FileSystem(FileSystem):
    """FileSystem wrapper for objects stored in AWS S3."""

    def __init__(self, path: Optional[str] = None,
                 bucket: Optional[str] = None,
                 prefix_or_key: Optional[str] = None,
                 client=None):

        if boto3 is None:
            raise ImportError('S3-backend requires `boto3` module to be'
                              f' installed ({boto3_import_error})')

        if bool(path) == bool(bucket) or bool(path) == bool(prefix_or_key):
            raise ValueError(
                'Either `path` or `(bucket, prefix_or_key)` should be'
                ' passed (but not both).'
            )

        if not path:
            path = os.path.join(bucket, prefix_or_key)
        else:
            bucket, prefix_or_key = split_s3_path(path)

        super().__init__(path)
        self.client = client or boto3.client('s3')
        self.bucket = bucket
        self.prefix_or_key = prefix_or_key

    # docstr-coverage:inherited
    def ls(self, recursive=False, files_only=False) -> List['S3FileSystem']:
        if recursive is False:
            raise NotImplementedError

        ls = self.client.list_objects(Bucket=self.bucket,
                                      Prefix=self.prefix_or_key)
        acc = [obj['Key'] for obj in ls.get('Contents', [])]
        return [
            S3FileSystem(bucket=self.bucket,
                         prefix_or_key=key,
                         client=self.client)
            for key in acc
        ]

    # docstr-coverage:inherited
    def read_yaml(self) -> dict:
        o = self.client.get_object(Bucket=self.bucket, Key=self.prefix_or_key)
        return yaml.load(o['Body'], Loader=yaml.CLoader)

    # docstr-coverage:inherited
    def write_yaml(self, doc: dict, **kwargs) -> None:
        return self.client.put_object(
            Body=yaml.dump(doc, sort_keys=False, **kwargs),
            Bucket=self.bucket,
            Key=self.prefix_or_key
        )

    # docstr-coverage:inherited
    def read_json(self) -> dict:
        o = self.client.get_object(Bucket=self.bucket, Key=self.prefix_or_key)
        return json.load(o['Body'])

    # docstr-coverage:inherited
    def write_json(self, doc: dict, **kwargs):
        return self.client.put_object(
            Body=json.dumps(doc, **kwargs),
            Bucket=self.bucket,
            Key=self.prefix_or_key
        )

    # docstr-coverage:inherited
    def import_as_python_module(self):
        extension = os.path.splitext(self.path)[1]
        with NamedTemporaryFile(suffix=extension) as tmp_fp:
            self.client.download_file(self.bucket,
                                      self.prefix_or_key,
                                      tmp_fp.name)
            module = _import_file_as_python_module(tmp_fp.name)
            return module


def fs_factory(path: str):
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
