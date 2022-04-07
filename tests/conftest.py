import os
import shutil

import boto3
import moto
import pytest


@pytest.fixture
def prepare_history_folder():
    local_path = 'tests/history/'
    shutil.rmtree(local_path, ignore_errors=True)
    os.mkdir(local_path)
    yield local_path
    shutil.rmtree(local_path)


@pytest.fixture(scope='session')
def require_s3_test_bucket():
    with moto.mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket')
        yield 'test-bucket'
