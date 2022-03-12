import pytest

from deirokay.fs import fs_factory


@pytest.mark.skip(reason='Need AWS credentials')
@pytest.mark.parametrize('src_path, dst_path', [
    ['tests/options.json', 's3://deirokay/options.yaml'],
    ['tests/options.json', 's3://deirokay/options.json'],
    ['tests/options.yaml', 's3://deirokay/options.yaml'],
    ['tests/options.yaml', 's3://deirokay/options.json'],
    ['s3://deirokay/options.json', '/tmp/options.yaml'],
    ['s3://deirokay/options.json', '/tmp/options.json'],
    ['s3://deirokay/options.yaml', '/tmp/options.yaml'],
    ['s3://deirokay/options.yaml', '/tmp/options.json'],
])
def test_fs_local(src_path, dst_path):
    src = fs_factory(src_path)
    dst = fs_factory(dst_path)

    doc = src.read_dict()
    print(doc)
    dst.write_dict(doc)

    assert src.read_dict() == dst.read_dict()
