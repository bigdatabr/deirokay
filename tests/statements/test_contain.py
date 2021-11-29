import pandas as pd

from deirokay import profile, validate


def test_rule_all():
    """
    Tests if ``rule = 'all'`` returns the expected outputs.
    Tries different combinations of states over three of the
    dummy dataframe's columns
    """
    df = pd.read_csv('test_contain.csv', sep=';')
    columns = [
        'test_rule_1',
        'test_rule_2',
        'test_rule_3'
    ]
    options = {
        'name': 'all_test_rule',
        'items': [
            {
                'scope': [''],
                'statements': [
                    {
                        'type': 'contain',
                        'rule': 'all',
                        'values': ['RJ', 'ES', 'SC', 'AC', 'SP']
                    }
                ]
            }
        ]
    }
    validation_result = {}
    for col in columns:
        options['items'][0]['scope'] = col
        validation_result[col] = validate(
            df[[col]], against=options, raise_exception=False
        )['items'][0]['statements'][0]['report']['result']
    assert validation_result['test_rule_1'] == 'pass'
    assert validation_result['test_rule_2'] == 'fail'
    assert validation_result['test_rule_3'] == 'pass'


def test_rule_only():
    """
    Tests if ``rule = 'only'`` returns the expected outputs.
    Tries different combinations of states over three of the
    dummy dataframe's columns
    """
    df = pd.read_csv('test_contain.csv', sep=';')
    columns = [
        'test_rule_1',
        'test_rule_2',
        'test_rule_3'
    ]
    options = {
        'name': 'only_test_rule',
        'items': [
            {
                'scope': [''],
                'statements': [
                    {
                        'type': 'contain',
                        'rule': 'only',
                        'values': ['RJ', 'ES', 'SC', 'AC', 'SP']
                    }
                ]
            }
        ]
    }
    validation_result = {}
    for col in columns:
        options['items'][0]['scope'] = col
        validation_result[col] = validate(
            df[[col]], against=options, raise_exception=False
        )['items'][0]['statements'][0]['report']['result']
    assert validation_result['test_rule_1'] == 'fail'
    assert validation_result['test_rule_2'] == 'pass'
    assert validation_result['test_rule_3'] == 'pass'


def test_max_min():
    """
    Tests if statement ``contain``'s checks about minimum
    and maximum quantities for each column/value are correct.
    Tries it with the global ``max/min_occurrences`` parameters
    and the ``occurrences_per_value`` parameter also.
    """
    df = pd.read_csv('test_contain.csv', sep=';')

    tests = {
        'min_global_1': {'min_occurrences': 3},
        'min_global_2': {'min_occurrences': 1},
        'max_global_1': {'max_occurrences': 4},
        'max_global_2': {'max_occurrences': 7},
        'occurrencess_per_value_1': {
            'min_occurrences': 2,
            'max_occurrences': 5,
            'occurrences_per_value': {
                'RJ': {'min_occurrences': 4},
                'ES': {'max_occurrences': 4}
            }
        },
        'occurrencess_per_value_2': {
            'min_occurrences': 3,
            'occurrences_per_value': {
                'SP': {'min_occurrences': 2}
            }
        }
    }
    validation_result = {}

    for test in tests.items():
        options = {
            'name': 'max_min_global_test',
            'items': [
                {
                    'scope': ['test_maxmin'],
                    'statements': [
                        {
                            'type': 'contain',
                            'rule': 'all_and_only',
                            'values': ['RJ', 'SP', 'ES']
                        }
                    ]
                }
            ]
        }

        statement = options['items'][0]['statements'][0]
        statement.update(test[1])
        options['items'][0]['statements'][0] = statement
        validation_result[test[0]] = validate(
            df[['test_maxmin']], against=options, raise_exception=False
        )['items'][0]['statements'][0]['report']['result']

    assert validation_result['min_global_1'] == 'fail'
    assert validation_result['min_global_2'] == 'pass'
    assert validation_result['max_global_1'] == 'fail'
    assert validation_result['max_global_2'] == 'pass'
    assert validation_result['occurrencess_per_value_1'] == 'fail'
    assert validation_result['occurrencess_per_value_2'] == 'pass'


def test_rule_not_contain():
    """
    Tests the extremal case of not containing some values, obtained
    by combining ``rule = 'all'`` and ``max_occurrences = 0``
    """
    df = pd.read_csv('test_contain.csv', sep=';')
    columns = [
        'test_not_contain'
    ]
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
    validation_result = {}
    for col in columns:
        options['items'][0]['scope'] = col
        validation_result[col] = validate(
            df[[col]], against=options, raise_exception=False
        )['items'][0]['statements'][0]['report']['result']
    assert validation_result['test_not_contain'] == 'pass'


def test_profile():
    """
    Tests if ``profile`` method outputs the expected value
    """
    df = pd.read_csv('test_contain.csv', sep=';')

    profile_doc = profile(
        df[['test_maxmin']], 'contain'
    )['items'][0]['statements']

    profile_contain = [x for x in profile_doc if x['type'] == 'contain'][0]
    expected_profile = {
        'type': 'contain',
        'rule': 'all_and_only',
        'values': {'test_maxmin': ['RJ', 'SP', 'ES']},
        'min_occurrences': {'test_maxmin': 2},
        'max_occurrences': {'test_maxmin': 5}
    }
    assert profile_contain == expected_profile
