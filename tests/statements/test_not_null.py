from deirokay import data_reader, validate


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
