from deirokay import data_reader, validate
from deirokay.config import DEFAULTS


def test_data_validation_with_jinja():

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

    validate(df, against=assertions,
             save_to='tests/history/VENDAS/19990101.json')
    validate(df, against=assertions,
             save_to='tests/history/VENDAS/19990102.json')
    validate(df, against=assertions,
             save_to='tests/history/VENDAS/19990103.json')
