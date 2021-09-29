import pytest

from deirokay import data_reader, validate
from deirokay.exceptions import ValidationError


def test_data_validation():

    df = data_reader(
        'tests/transactions_sample.csv',
        encoding='iso-8859-1', sep=';',
        options_json='tests/options.json'
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
                    }
                ]
            }
        ]
    }

    with pytest.raises(ValidationError):
        validate(df, against=assertions, save_to='report.json')


@pytest.mark.skip(reason='Consumer for Custom Stmt Not implemented')
def test_custom_statement():
    df = data_reader(
        'tests/transactions_sample.csv',
        encoding='iso-8859-1', sep=';',
        options_json='tests/options.json'
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
                        'x': 3000
                    }
                ]
            }
        ]
    }

    validate(df, against=assertions)
