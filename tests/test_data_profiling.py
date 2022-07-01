from pprint import pprint

from deirokay import data_reader, profile, validate
from deirokay.enums import Backend


def test_profiling():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=Backend.PANDAS
    )

    validation_document = profile(df, 'transactions_sample')
    pprint(validation_document)

    validate(df, against=validation_document)
