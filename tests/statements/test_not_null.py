from pprint import pprint

import pytest

from deirokay import data_reader, profile, validate
from deirokay.enums import Backend


@pytest.mark.parametrize("backend", list(Backend))
def test_not_null_statement(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "items": [
            {
                "scope": "NUM_TRANSACAO01",
                "statements": [
                    {
                        "type": "not_null",
                        "at_least_%": 90,
                    }
                ],
            },
            {
                "scope": ["WERKS01", "HR_TRANSACAO01"],
                "statements": [
                    {"type": "not_null", "at_least_%": 90.0, "multicolumn_logic": "any"}
                ],
            },
        ]
    }

    validate(df, against=assertions)


@pytest.mark.parametrize("backend", list(Backend))
def test_profile_wont_generate_useless_statement(backend):
    """Prevent statement generation when `at_least_%` is 0."""
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    validation_doc = profile(df, "sample")
    pprint(validation_doc)

    # Check that the statement was not generated for "EMPTY" column
    scope_empty = next(filter(lambda x: x["scope"] == "EMPTY", validation_doc["items"]))
    assert (
        len([stmt for stmt in scope_empty["statements"] if stmt["type"] == "not_null"])
        == 0
    )
