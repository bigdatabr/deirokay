from datetime import datetime

import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend
from deirokay.fs import fs_factory, split_s3_path
from deirokay.history_template import series_from_fs


@pytest.mark.parametrize('backend', list(Backend))
def test_data_validation_with_jinja(prepare_history_folder, backend):

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=backend
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'PROD_VENDA'],
                'alias': 'werks_prod',
                'statements': [
                    {'type': 'row_count',
                     'min': '{{ 0.95 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.5, true))|float }}',  # noqa: E501
                     'max': '{{ 1.05 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.6, true))|float }}'}  # noqa: E501
                ]
            }
        ]
    }

    doc = validate(df, against=assertions,
                   save_to=prepare_history_folder,
                   current_date=datetime(1999, 1, 1))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(18.525)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(20.58)

    doc = validate(df, against=assertions,
                   save_to=prepare_history_folder,
                   current_date=datetime(1999, 1, 2))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)

    doc = validate(df, against=assertions,
                   save_to=prepare_history_folder,
                   current_date=datetime(1999, 1, 3))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)


@pytest.fixture
def prepare_history_s3(require_s3_test_bucket):
    import boto3
    s3 = boto3.client('s3')

    def delete_s3_prefix(bucket, prefix):
        for obj in (s3.list_objects(Bucket=bucket, Prefix=prefix)
                    .get('Contents', [])):
            s3.delete_object(Bucket=bucket, Key=obj['Key'])

    s3_path = f's3://{require_s3_test_bucket}/'
    bucket, prefix = split_s3_path(s3_path)
    delete_s3_prefix(bucket, prefix)
    yield s3_path
    delete_s3_prefix(bucket, prefix)


@pytest.mark.parametrize('backend', list(Backend))
def test_data_validation_with_jinja_using_s3(monkeypatch, prepare_history_s3,
                                             backend):
    # Reduce the number of retrieved objects to test pagination
    monkeypatch.setattr('deirokay.fs.S3FileSystem.LIST_OBJECTS_MAX_KEYS', 1)

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=backend
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'PROD_VENDA'],
                'alias': 'werks_prod',
                'statements': [
                    {'type': 'row_count',
                     'min': '{{ 0.95 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.5, true))|float }}',  # noqa: E501
                     'max': '{{ 1.05 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.6, true))|float }}'}  # noqa: E501
                ]
            }
        ]
    }

    doc = validate(df, against=assertions,
                   save_to=prepare_history_s3,
                   current_date=datetime(1999, 1, 1))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(18.525)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(20.58)

    doc = validate(df, against=assertions,
                   save_to=prepare_history_s3,
                   current_date=datetime(1999, 1, 2))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)

    doc = validate(df, against=assertions,
                   save_to=prepare_history_s3,
                   current_date=datetime(1999, 1, 3))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)


@pytest.fixture
def _create_files_with_same_prefixes(require_s3_test_bucket):
    import boto3
    s3 = boto3.client('s3')
    bucket = require_s3_test_bucket
    s3.put_object(Bucket=bucket, Key='path/file.json', Body='{"a":"b"}')
    s3.put_object(Bucket=bucket, Key='path_2/file.json', Body='{"c":"d"}')
    yield
    s3.delete_object(Bucket=bucket, Key='path_2/file.json')
    s3.delete_object(Bucket=bucket, Key='path/file.json')


def test_s3_paths_with_same_prefixes(require_s3_test_bucket,
                                     _create_files_with_same_prefixes):
    """Tests the problem of paths with same prefix (one being the
    prefix of another).

    Ref: https://github.com/bigdatabr/deirokay/issues/37
    """
    bucket = require_s3_test_bucket

    folder = fs_factory(f's3://{bucket}/')
    files = series_from_fs('path', 10, folder)
    assert len(files) == 1
