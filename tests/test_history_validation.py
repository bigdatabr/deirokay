from datetime import datetime

import pytest

from deirokay import data_reader, validate
from deirokay.fs import split_s3_path


def test_data_validation_with_jinja(prepare_history_folder):

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
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


def test_data_validation_with_jinja_using_s3(monkeypatch, prepare_history_s3):
    # Reduce the number of retrieved objects to test pagination
    monkeypatch.setattr('deirokay.fs.S3FileSystem.LIST_OBJECTS_MAX_KEYS', 1)

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
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
