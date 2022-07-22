import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend
from deirokay.statements.builtin import StatisticInInterval


@pytest.mark.parametrize('backend', list(Backend))
@pytest.mark.parametrize(
    'scope, statistic, intervals, result',
    [
        ('PROD_VENDA', 'count', {'==': 17}, 'fail'),
        ('PROD_VENDA', 'count', {'==': 20}, 'pass'),
        ('PROD_VENDA', 'count', {'=~': 20.00000001}, 'pass'),
        ('PROD_VENDA', 'count', {'!~': 19.99999}, 'pass'),
        ('PROD_VENDA', 'nunique', {'==': 17}, 'fail'),
        ('COD_MERC_SERV02', 'max', {'<=': 7100900}, 'pass'),
        ('COD_SETVENDAS', 'mode', {'>=': 6001628,
                                   '<=': 6001628}, 'pass'),
        ('NUMERO_PDV_ORIGIN', 'min', {'>=': 10.5,
                                      '<=': 11.5}, 'pass'),
        ('NUMERO_PDV_ORIGIN', 'max', {'>=': 12,
                                      '<=': 12.5}, 'pass'),
    ]
)
def test_statistic_in_interval(scope, statistic, intervals, result, backend):
    df = data_reader('tests/transactions_sample.csv',
                     options='tests/options.yaml',
                     backend=backend)
    assertions = {
        'name': 'statistic_in_interval',
        'items': [
            {
                'scope': scope,
                'statements': [
                    {
                        'type': 'statistic_in_interval',
                        'statistic': statistic,
                        **intervals
                    }
                ]
            }
        ]
    }
    report = (validate(df, against=assertions, raise_exception=False)
              ['items'][0]['statements'][0]['report'])
    print(report['detail']['value'])
    assert report['result'] == result


@pytest.mark.parametrize('backend', list(Backend))
def test_profile(backend):
    df = data_reader('tests/transactions_sample.csv',
                     options='tests/options.yaml',
                     backend=backend)

    cls = StatisticInInterval.attach_backend(backend)

    generated_prof = cls.profile(df[['NUM_TRANSACAO01']])

    assertions = {
        'name': 'profiling',
        'items': [{
            'scope': 'NUM_TRANSACAO01',
            'statements': [generated_prof]
        }]
    }

    report = (validate(df, against=assertions, raise_exception=False)
              ['items'][0]['statements'][0]['report'])
    print(report['detail']['value'])
    assert report['result'] == 'pass'
