from pprint import pprint

import pytest

from deirokay import data_reader, profile, validate
from deirokay.enums import Backend


@pytest.mark.parametrize("backend", list(Backend))
def test_profiling(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    validation_document = profile(df, "transactions_sample")
    pprint(validation_document)

    validate(df, against=validation_document)
