import pandas as pd
import pytest

from deirokay import validate
from deirokay.statements import Contain


@pytest.mark.parametrize('rule, scope, result',
                         [('all', 'test_rule_1', 'pass'),
                          ('all', 'test_rule_2', 'fail'),
                          ('all', 'test_rule_3', 'pass'),
                          ('only', 'test_rule_1', 'fail'),
                          ('only', 'test_rule_2', 'pass'),
                          ('only', 'test_rule_3', 'pass')])
def test_rules(rule, scope, result):
    df = pd.read_csv('tests/statements/test_contain.csv', sep=';')
    options = {
        'name': 'all_test_rule',
        'items': [
            {
                'scope': scope,
                'statements': [
                    {
                        'type': 'contain',
                        'rule': rule,
                        'values': ['RJ', 'ES', 'SC', 'AC', 'SP']
                    }
                ]
            }
        ]
    }
    assert (
        validate(df, against=options, raise_exception=False)
        ['items'][0]['statements'][0]['report']['result']
    ) == result


@pytest.mark.parametrize('occurrences, result',
                         [({'min_occurrences': 3}, 'fail'),
                          ({'min_occurrences': 1}, 'pass'),
                          ({'max_occurrences': 4}, 'fail'),
                          ({'max_occurrences': 7}, 'pass'),
                          ({
                              'min_occurrences': 2,
                              'max_occurrences': 5,
                              'occurrences_per_value': {
                                  'RJ': {'min_occurrences': 4},
                                  'ES': {'max_occurrences': 4}
                              }
                          }, 'fail'),
                          ({
                              'min_occurrences': 3,
                              'occurrences_per_value': {
                                  'SP': {'min_occurrences': 2}
                              }
                          }, 'pass')])
def test_max_min(occurrences, result):
    """
    Tests if statement `contain`'s checks about minimum
    and maximum quantities for each column/value are correct.
    Tries it with the global `max/min_occurrences` parameters
    and the `occurrences_per_value` parameter also.
    """
    df = pd.read_csv('tests/statements/test_contain.csv', sep=';')

    options = {
        'name': 'max_min_global_test',
        'items': [
            {
                'scope': 'test_maxmin',
                'statements': [
                    dict({
                        'type': 'contain',
                        'rule': 'all_and_only',
                        'values': ['RJ', 'SP', 'ES']
                    },
                        **occurrences)
                ]
            }
        ]
    }
    assert (
        validate(df, against=options, raise_exception=False)
        ['items'][0]['statements'][0]['report']['result']
    ) == result


def test_rule_not_contain():
    """
    Tests the extremal case of not containing some values, obtained
    by combining `rule = 'all'` and `max_occurrences = 0`
    """
    df = pd.read_csv('tests/statements/test_contain.csv', sep=';')
    options = {
        'name': 'all_not_contain_test_rule',
        'items': [
            {
                'scope': ['test_not_contain'],
                'statements': [
                    {
                        'type': 'contain',
                        'rule': 'all',
                        'values': ['AC', 'AM'],
                        'max_occurrences': 0
                    }
                ]
            }
        ]
    }
    assert (
        validate(df, against=options, raise_exception=False)
        ['items'][0]['statements'][0]['report']['result']
    ) == 'pass'


def test_profile():
    """
    Tests if `profile` method outputs the expected value
    """
    df = pd.read_csv('tests/statements/test_contain.csv', sep=';')

    prof = Contain.profile(df[['test_maxmin']])

    expected_profile = {
        'type': 'contain',
        'rule': 'all_and_only',
        'values': ['RJ', 'SP', 'ES'],
        'min_occurrences': 2,
        'max_occurrences': 5}
    }
    assert prof == expected_profile
