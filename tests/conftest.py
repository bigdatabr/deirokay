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
