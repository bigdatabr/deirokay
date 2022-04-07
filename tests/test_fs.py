import pytest

from deirokay.fs import fs_factory


@pytest.mark.parametrize('src_path, dst_path', [
    ['tests/options.json', 's3://{bucket}/options.yaml'],
    ['tests/options.json', 's3://{bucket}/options.json'],
    ['tests/options.yaml', 's3://{bucket}/options.yaml'],
    ['tests/options.yaml', 's3://{bucket}/options.json'],
    ['s3://{bucket}/options.json', '/tmp/options.yaml'],
    ['s3://{bucket}/options.json', '/tmp/options.json'],
    ['s3://{bucket}/options.yaml', '/tmp/options.yaml'],
    ['s3://{bucket}/options.yaml', '/tmp/options.json'],
])
def test_fs_local(src_path, dst_path, require_s3_test_bucket):
    src = fs_factory(src_path.replace('{bucket}', require_s3_test_bucket))
    dst = fs_factory(dst_path.replace('{bucket}', require_s3_test_bucket))

    doc = src.read_dict()
    print(doc)
    dst.write_dict(doc)

    assert src.read_dict() == dst.read_dict()
