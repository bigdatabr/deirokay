from pprint import pprint
from deirokay import data_reader, profile


def test_profile_wont_generate_useless_statement():
    """Prevent statement generation when `at_least_%` is 0."""
    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    validation_doc = profile(df, 'sample')
    pprint(validation_doc)

    # Check that the statement was not generated for
    # "DT_OPERACAO01" column
    scope_empty = next(filter(
        lambda x: x['scope'] == 'DT_OPERACAO01',
        validation_doc['items']
    ))
    assert len([
        stmt for stmt in scope_empty['statements']
        if stmt['type'] == 'unique'
    ]) == 0
