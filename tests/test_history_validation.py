import os
import shutil
from datetime import datetime

import pytest

from deirokay import data_reader, validate
from deirokay.config import DEFAULTS


@pytest.fixture
def prepare_history_folder():
    shutil.rmtree('tests/history', ignore_errors=True)
    os.mkdir('tests/history')
    yield
    shutil.rmtree('tests/history')


def test_data_validation_with_jinja(prepare_history_folder):

    DEFAULTS['log_folder'] = 'tests/history/'

    df = data_reader(
        'tests/transactions_sample.csv',
        options_json='tests/options.json'
    )

    assertions = {
        'name': 'VENDAS',
        'items': [
            {
                'scope': ['WERKS01', 'PROD_VENDA'],
                'alias': 'werks_prod',
                'statements': [
                    {'type': 'row_count',
                     'min': '{{ 0.95 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.5, true)) }}',  # noqa: E501
                     'max': '{{ 1.05 * (series("VENDAS", 3).werks_prod.row_count.rows.mean()|default(19.6, true)) }}'}  # noqa: E501
                ]
            }
        ]
    }

    doc = validate(df, against=assertions, save_to='tests/history/',
                   current_date=datetime(1999, 1, 1))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(18.525)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(20.58)

    doc = validate(df, against=assertions, save_to='tests/history/',
                   current_date=datetime(1999, 1, 2))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)

    doc = validate(df, against=assertions, save_to='tests/history/',
                   current_date=datetime(1999, 1, 3))
    assert doc['items'][0]['statements'][0]['min'] == pytest.approx(19.0)
    assert doc['items'][0]['statements'][0]['max'] == pytest.approx(21.0)
