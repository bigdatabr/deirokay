import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend
from deirokay.statements.builtin import RowCount


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "scope, params, result",
    [
        ("WERKS01", {"min": 20, "max": 20}, "pass"),
        ("WERKS01", {"min": 21}, "fail"),
        ("WERKS01", {"distinct": False, "max": 19}, "fail"),
        ("COD_MERC_SERV02", {"distinct": False, "max": 20}, "pass"),
        ("COD_MERC_SERV02", {"distinct": True, "min": 10, "max": 10}, "pass"),
        ("COD_MERC_SERV02", {"distinct": True, "min": 11, "max": 11}, "fail"),
        ("COD_MERC_SERV02", {"distinct": True, "min": 9, "max": 9}, "fail"),
    ],
)
def test_row_count(scope, params, result, backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.yaml", backend=backend
    )
    assertions = {
        "name": "test_row_count",
        "items": [
            {"scope": scope, "statements": [dict({"type": "row_count"}, **params)]}
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == result


@pytest.mark.parametrize("backend", list(Backend))
def test_profile_over_multi_columns(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.yaml", backend=backend
    )
    doc = RowCount.attach_backend(backend).profile(df)
    assert doc == {"type": "row_count", "min": 20, "max": 20}


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "column, expected",
    [
        ("WERKS01", {"type": "row_count", "distinct": True, "min": 1, "max": 1}),
        (
            "COD_MERC_SERV02",
            {"type": "row_count", "distinct": True, "min": 10, "max": 10},
        ),
        ("COD_SETVENDAS", {"type": "row_count", "distinct": True, "min": 6, "max": 6}),
    ],
)
def test_profile_over_single_column(column, expected, backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.yaml", backend=backend
    )
    doc = RowCount.attach_backend(backend).profile(df[[column]])
    assert doc == expected
