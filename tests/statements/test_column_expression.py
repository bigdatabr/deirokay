import pytest

from deirokay import data_reader, validate


@pytest.mark.parametrize(
    'scope, expressions, at_least, at_most, result',
    [
        (['c1', 'c2'], 'c1 + 5 == c2', 100, 100, 'pass'),
        (['c1', 'c2'], 'c1 ** 2 == c2', 100, 100, 'fail'),
        (
            ['c1', 'c3'],
            'c3 - 0.01 <= c1 <= c3 + 0.01',
            100,
            100,
            'pass'
        ),
        (['c4', 'c5'], 'c4 + c4 == c5', 100, 100, 'pass'),
        (['c5', 'c6'], 'c5 == c6', 80, 100, 'pass'),
        (['c5', 'c6'], 'c5 == c6', 81, 100, 'fail'),
        (
            ['c1', 'c2', 'c4', 'c5'],
            ['c1 + 5 == c2', 'c4 + c4 == c5'],
            100,
            100,
            'pass'
        ),
        (['c1', 'c8'], 'c1 + 0.1 == c8', 100, 100, 'pass'),
        (['c1', 'c9'], 'c1 + 0.1 == c9', 60, 100, 'pass'),
        (['c1', 'c9'], 'c1 + 0.1 == c9', 61, 100, 'fail')
    ])
def test_column_expression(scope, expressions, at_least, at_most, result):
    df = data_reader(
        'tests/statements/test_column_expression.csv',
        options='tests/statements/test_column_expression_options.yaml'
    )
    assertions = {
        'name': 'expressions_test',
        'items': [
            {
                'scope': scope,
                'statements': [
                    {
                        'type': 'column_expression',
                        'expressions': expressions,
                        'at_least_%': at_least,
                        'at_most_%': at_most
                    }
                ]
            }
        ]
    }
    assert (
        validate(df, against=assertions, raise_exception=False)
        ['items'][0]['statements'][0]['report']['result']
    ) == result


@pytest.mark.parametrize(
    'scope, expressions, at_least, at_most, result',
    [
        (['c1', 'c3'], 'c1 =~ c3', 100, 100, 'pass'),
        (['c1', 'c2'], 'c1 =~ c2', 100, 100, 'fail'),
        (['c1', 'c2', 'c3'], 'c1 =~ c3 =~ c2', 100, 100, 'fail'),
        (['c1', 'c2', 'c3'], 'c1 =~ c3 + c2', 100, 100, 'fail'),
        (['c1', 'c2', 'c3'], 'c1 =~ c3 + 0.0001', 100, 100, 'pass'),
        (['c1', 'c2', 'c3'], 'c1 =~ c3 <= 6', 100, 100, 'pass'),
        (['c1', 'c7'], 'c1 =~ c7', 80, 100, 'pass'),
        (['c1', 'c7'], 'c1 =~ c7', 81, 100, 'fail'),
        (['c1', 'c8'], 'c1 =~ c8', 100, 100, 'pass'),
        (['c1', 'c9'], 'c1 =~ c9', 60, 100, 'pass')
    ])
def test_isclose_expression(scope, expressions, at_least, at_most, result):
    df = data_reader(
        'tests/statements/test_column_expression.csv',
        options='tests/statements/test_column_expression_options.yaml'
    )
    assertions = {
        'name': 'expressions_isclose_test',
        'items': [
            {
                'scope': scope,
                'statements': [
                    {
                        'type': 'column_expression',
                        'expressions': expressions,
                        'at_least_%': at_least,
                        'at_most_%': at_most,
                        'atol': 0.1
                    }
                ]
            }
        ]
    }
    assert (
        validate(df, against=assertions, raise_exception=False)
        ['items'][0]['statements'][0]['report']['result']
    ) == result
