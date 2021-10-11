from deirokay import data_reader, validate, profile


def test_profiling():

    df = data_reader(
        'tests/transactions_sample.csv',
        options_json='tests/options.json'
    )

    validation_document = profile(df, 'transactions_sample')
    validate(df, against=validation_document)
