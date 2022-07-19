import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend
from deirokay.statements.builtin import Unique


@pytest.mark.parametrize('backend', list(Backend))
def test_unique(backend):
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=backend
    )
    assertions = {
        'name': 'test_unique',
        'items': [
            {
                'scope': df.columns.tolist(),
                'statements': [
                    {
                        'type': 'unique'
                    }
                ]
            }
        ]
    }
    result = validate(df, against=assertions, raise_exception=False)
    assert (
        result['items'][0]['statements'][0]['report']['detail'] == {
            'unique_rows': 20,
            'unique_rows_%': 100.0,
        }
    )


@pytest.mark.parametrize('backend', list(Backend))
def test_not_unique(backend):
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=backend
    )
    assertions = {
        'name': 'test_not_unique',
        'items': [
            {
                'scope': 'PROD_VENDA',
                'statements': [
                    {
                        'type': 'unique'
                    }
                ]
            }
        ]
    }
    result = validate(df, against=assertions, raise_exception=False)
    assert (
        result['items'][0]['statements'][0]['report']['detail'] == {
            'unique_rows': 9,
            'unique_rows_%': 45.0,
        }
    )


@pytest.mark.parametrize('backend', list(Backend))
def test_profile_wont_generate_useless_statement(backend):
    """Prevent statement generation when `at_least_%` is 0."""
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=backend
    )[['DT_OPERACAO01']]

    with pytest.raises(NotImplementedError):
        Unique.attach_backend(backend).profile(df)
