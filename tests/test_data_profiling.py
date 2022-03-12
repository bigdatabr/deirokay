from pprint import pprint

from deirokay import data_reader, profile, validate


def test_profiling():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    validation_document = profile(df, 'transactions_sample')
    pprint(validation_document)

    validate(df, against=validation_document)
