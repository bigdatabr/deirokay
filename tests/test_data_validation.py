import pytest

from deirokay import data_reader, validate
from deirokay.enums import SeverityLevel
from deirokay.exceptions import ValidationError
from deirokay.fs import split_s3_path


def test_data_invalidation_from_dict():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'DT_OPERACAO01'],
                'statements': [
                    {
                        'type': 'unique',
                        'at_least_%': 1.0,
                    },
                    {
                    "type": "regex",
                    "pattern": "\d{4}",
                    "at_least_%": 80.0
                    }
                ]
            }
        ]
    }

    with pytest.raises(ValidationError):
        validate(df, against=assertions)


def test_data_validation_from_yaml():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.yaml'
    )

    validate(df, against='tests/assertions.yml')


def test_data_validation_from_json():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    validate(df, against='tests/assertions.json')


def test_not_null_statement():
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'items': [
            {
                'scope': 'NUM_TRANSACAO01',
                'statements': [
                    {
                        'type': 'not_null',
                        'at_least_%': 90,
                    }
                ]
            },
            {
                'scope': ['WERKS01', 'HR_TRANSACAO01'],
                'statements': [
                    {
                        'type': 'not_null',
                        'at_least_%': 90.0,
                        'multicolumn_logic': 'any'
                    }
                ]
            },
        ]
    }

    validate(df, against=assertions)


def test_custom_statement():
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': 'NUM_TRANSACAO01',
                'statements': [
                    {
                        'type': 'custom',
                        'location': 'tests/custom_statement.py'
                                    '::ThereAreValuesGreaterThanX',
                        'x': 2
                    }
                ]
            }
        ]
    }

    validate(df, against=assertions)


@pytest.fixture
def prepare_s3_custom_statement():
    local_path = 'tests/custom_statement.py'
    s3_path = 's3://bigdata-momo/temp/custom_statement.py'
    bucket, key = split_s3_path(s3_path)

    import boto3
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, key)
    yield s3_path
    s3.delete_object(Bucket=bucket, Key=key)


@pytest.mark.skip(reason='Need AWS credentials')
def test_custom_statement_from_s3(prepare_s3_custom_statement):
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': 'NUM_TRANSACAO01',
                'statements': [
                    {
                        'type': 'custom',
                        'location': (
                            prepare_s3_custom_statement +
                            '::ThereAreValuesGreaterThanX'
                        ),
                        'x': 2
                    }
                ]
            }
        ]
    }

    validate(df, against=assertions)


def test_data_validation_with_jinja():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'PROD_VENDA'],
                'statements': [
                    {'type': 'unique', 'at_least_%': '{{ 40.0 }}'},
                    {'type': 'not_null', 'at_least_%': 95.0},
                    {'type': 'row_count', 'min': '{{ 18 }}', 'max': 22}
                ]
            }
        ]
    }

    validate(df, against=assertions)


def test_data_validation_with_levels():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'PROD_VENDA'],
                'statements': [
                    {
                        'type': 'unique',
                        'severity': SeverityLevel.WARNING,
                        'at_least_%': '{{ 100.0 }}'
                    },
                    {
                        'type': 'not_null',
                        'at_least_%': 100.0,
                    },
                    {
                        'type': 'row_count',
                        'min': '{{ 18 }}',
                        'max': 22
                    }
                ]
            }
        ]
    }

    validate(df, against=assertions)
