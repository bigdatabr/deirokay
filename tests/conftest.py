import os
import shutil

import pytest


@pytest.fixture
def prepare_history_folder():
    local_path = 'tests/history/'
    shutil.rmtree(local_path, ignore_errors=True)
    os.mkdir(local_path)
    yield local_path
    shutil.rmtree(local_path)


@pytest.fixture
def require_s3_test_bucket():
    s3_test_bucket = os.environ.get('DEIROKAY_TEST_BUCKET')
    if not s3_test_bucket:
        pytest.skip('DEIROKAY_TEST_BUCKET variable not set')

    return s3_test_bucket
