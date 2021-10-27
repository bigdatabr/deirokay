import importlib
import json
import os
import re
from os.path import splitext
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, Optional

import yaml

boto3_import_error = None
try:
    import boto3
except ImportError as e:
    boto3 = None
    boto3_import_error = e


BUCKET_KEY_REGEX = re.compile(r's3:\/\/([\w\-]+)\/([\w\-\/.]+)')


def split_s3_path(s3_path: str):
    bucket, key = BUCKET_KEY_REGEX.findall(s3_path)[0]
    return bucket, key


def _import_file_as_python_module(path_to_file):
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
    def __init__(self, path: str):
        self.path = path

    def ls(self, recursive=False, files_only=False):
        raise NotImplementedError

    def read_dict(self, *args, **kwargs) -> dict:
        extension = splitext(self.path)[1].lower()
        if extension == '.json':
            return self.read_json(*args, **kwargs)
        elif extension in ('.yaml', '.yml'):
            return self.read_yaml(*args, **kwargs)
        raise NotImplementedError(f'No parser for file type: {extension}')

    def write_dict(self, *args, **kwargs):
        extension = splitext(self.path)[1].lower()
        if extension == '.json':
            return self.write_json(*args, **kwargs)
        elif extension in ('.yaml', '.yml'):
            return self.write_yaml(*args, **kwargs)
        raise NotImplementedError(f'No serializer for file type: {extension}')

    def read_yaml(self):
        raise NotImplementedError

    def write_yaml(self, doc: dict, **kwargs):
        raise NotImplementedError

    def read_json(self) -> dict:
        raise NotImplementedError

    def write_json(self, doc: dict, **kwargs):
        raise NotImplementedError

    def isdir(self):
        raise NotImplementedError

    def mkdir(self, *args, **kwargs):
        raise NotImplementedError

    def import_as_python_module(self):
        raise NotImplementedError

    def __truediv__(self, rest: str):
        if isinstance(rest, str):
            cls = type(self)
            return cls(os.path.join(self.path, rest))
        raise TypeError()

    def __lt__(self, other: 'FileSystem'):
        return self.path.__lt__(other.path)

    def __str__(self):
        return self.path


class LocalFileSystem(FileSystem):
    def ls(self, recursive=False, files_only=False) -> List['LocalFileSystem']:
        if recursive is False:
            raise NotImplementedError

        acc = []
        for parent, folders, files in os.walk(self.path):
            if not files_only:
                acc += [os.path.join(parent, folder) for folder in folders]
            acc += [os.path.join(parent, file) for file in files]
        return [LocalFileSystem(path) for path in acc]

    def read_yaml(self) -> dict:
        with open(self.path) as fp:
            return yaml.load(fp, Loader=yaml.CLoader)

    def write_yaml(self, doc: dict, **kwargs) -> None:
        with open(self.path, 'w') as fp:
            yaml.dump(doc, fp, sort_keys=False, **kwargs)

    def read_json(self) -> dict:
        with open(self.path) as fp:
            return json.load(fp)

    def write_json(self, doc: dict, **kwargs) -> None:
        with open(self.path, 'w') as fp:
            json.dump(doc, fp, **kwargs)

    def import_as_python_module(self):
        module = _import_file_as_python_module(self.path)
        return module

    def isdir(self):
        return os.path.isdir(self.path)

    def mkdir(self, *args, **kwargs):
        return Path(self.path).mkdir(*args, **kwargs)


class S3FileSystem(FileSystem):
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

    def read_yaml(self) -> dict:
        o = self.client.get_object(Bucket=self.bucket, Key=self.prefix_or_key)
        return yaml.load(o['Body'], Loader=yaml.CLoader)

    def write_yaml(self, doc: dict, **kwargs) -> None:
        return self.client.put_object(
            Body=yaml.dump(doc, sort_keys=False, **kwargs),
            Bucket=self.bucket,
            Key=self.prefix_or_key
        )

    def read_json(self) -> dict:
        o = self.client.get_object(Bucket=self.bucket, Key=self.prefix_or_key)
        return json.load(o['Body'])

    def write_json(self, doc: dict, **kwargs):
        return self.client.put_object(
            Body=json.dumps(doc, **kwargs),
            Bucket=self.bucket,
            Key=self.prefix_or_key
        )

    def import_as_python_module(self):
        extension = os.path.splitext(self.path)[1]
        with NamedTemporaryFile(suffix=extension) as tmp_fp:
            self.client.download_file(self.bucket,
                                      self.prefix_or_key,
                                      tmp_fp.name)
            module = _import_file_as_python_module(tmp_fp.name)
            return module


def fs_factory(path: str):
    if path.startswith('s3://'):
        return S3FileSystem(path)
    else:
        return LocalFileSystem(path)
