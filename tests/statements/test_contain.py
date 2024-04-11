import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend
from deirokay.statements.builtin import Contain


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "rule, scope, result",
    [
        ("all", "test_rule_1", "pass"),
        ("all", "test_rule_2", "fail"),
        ("all", "test_rule_3", "fail"),
        ("only", "test_rule_1", "fail"),
        ("only", "test_rule_2", "pass"),
        ("only", "test_rule_3", "fail"),
    ],
)
def test_rules(rule, scope, result, backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "all_test_rule",
        "items": [
            {
                "scope": scope,
                "statements": [
                    {
                        "type": "contain",
                        "rule": rule,
                        "values": ["RJ", "ES", "SC", "AC", "SP"],
                        "parser": {"dtype": "string"},
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == result


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "occurrences, result",
    [
        ({"min_occurrences": 3}, "fail"),
        ({"min_occurrences": 1}, "pass"),
        ({"max_occurrences": 4}, "fail"),
        ({"max_occurrences": 7}, "pass"),
        (
            {
                "min_occurrences": 2,
                "max_occurrences": 5,
                "occurrences_per_value": [
                    {"values": ["RJ"], "min_occurrences": 4},
                    {"values": ["ES"], "max_occurrences": 4},
                ],
            },
            "fail",
        ),
        (
            {
                "min_occurrences": 3,
                "occurrences_per_value": [{"values": ["SP"], "min_occurrences": 2}],
            },
            "pass",
        ),
    ],
)
def test_max_min(occurrences, result, backend):
    """
    Tests if statement `contain`'s checks about minimum
    and maximum quantities for each column/value are correct.
    Tries it with the global `max/min_occurrences` parameters
    and the `occurrences_per_value` parameter also.
    """
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )

    assertions = {
        "name": "max_min_global_test",
        "items": [
            {
                "scope": "test_maxmin",
                "statements": [
                    dict(
                        {
                            "type": "contain",
                            "rule": "all_and_only",
                            "values": ["RJ", "SP", "ES"],
                            "parser": {"dtype": "string"},
                        },
                        **occurrences
                    )
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == result


@pytest.mark.parametrize("backend", list(Backend))
def test_rule_not_contain(backend):
    """
    Tests the extremal case of not containing some values, obtained
    by combining `rule = 'all'` and `max_occurrences = 0`
    """
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "all_not_contain_test_rule",
        "items": [
            {
                "scope": ["test_not_contain"],
                "statements": [
                    {
                        "type": "contain",
                        "rule": "all",
                        "values": ["AC", "AM"],
                        "parser": {"dtype": "string"},
                        "max_occurrences": 0,
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == "pass"


@pytest.mark.parametrize("backend", list(Backend))
def test_profile(backend):
    """
    Tests if `profile` method outputs the expected value
    """
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )

    contain = Contain.attach_backend(backend)
    generated_prof = contain.profile(df[["test_maxmin"]])

    expected_profile = {
        "type": "contain",
        "rule": "all",
        "values": ["ES", "RJ", "SP"],
        "parser": {"dtype": "string"},
        "min_occurrences": 2,
    }

    assert generated_prof == expected_profile

    assertions = {
        "name": "profiling",
        "items": [{"scope": "test_maxmin", "statements": [generated_prof]}],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == "pass"


@pytest.mark.parametrize("backend", list(Backend))
def test_multicolumn(backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "all_test_rule",
        "items": [
            {
                "scope": ["test_rule_1", "test_rule_2"],
                "statements": [
                    {
                        "type": "contain",
                        "rule": "all",
                        "values": [["RJ", "RJ"], ["ES", "ES"]],
                        "multicolumn": True,
                        "parsers": 2 * [{"dtype": "string"}],
                        "occurrences_per_value": [
                            {
                                "values": [("RJ", "RJ"), ("ES", "ES")],
                                "min_occurrences": 1,
                            }
                        ],
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == "pass"


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "scope, rule, result, values",
    [
        ("test_rule_1", "all", "pass", ["AC", "SP", None]),
        ("test_rule_1", "only", "fail", [None]),
        ("test_rule_2", "all", "fail", [None]),
        ("test_not_contain", "all", "fail", ["RJ", "SP", "ES", None]),
    ],
)
def test_null_values(scope, rule, result, values, backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "all_test_rule",
        "items": [
            {
                "scope": scope,
                "statements": [
                    {
                        "type": "contain",
                        "rule": rule,
                        "values": values,
                        "parser": {"dtype": "string"},
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == result


@pytest.mark.parametrize("backend", list(Backend))
@pytest.mark.parametrize(
    "scope, rule, result, tolerance_perc, values",
    [
        ("test_rule_1", "only", "pass", 20, ["RJ", "ES", "SC", "AC", None]),
        ("test_rule_1", "only", "fail", 10, ["RJ", "ES", "SC", "AC", None]),
        ("test_rule_2", "only", "fail", 30, ["RJ", "ES"]),
        ("test_rule_2", "only", "pass", 34, ["RJ", "ES"]),
        ("test_rule_2", "all", "fail", 20, ["RJ", "ES", "SC", "MG"]),
        ("test_rule_2", "all", "pass", 25, ["RJ", "ES", "SC", "MG"]),
    ],
)
def test_allowed_perc_error(scope, rule, result, tolerance_perc, values, backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "test_rule_1",
        "items": [
            {
                "scope": scope,
                "statements": [
                    {
                        "type": "contain",
                        "rule": rule,
                        "values": values,
                        "parser": {"dtype": "string"},
                        "tolerance_%": tolerance_perc,
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == result


@pytest.mark.parametrize("backend", list(Backend))
def test_allowed_perc_error_and_min(backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "test_rule_1",
        "items": [
            {
                "scope": "test_maxmin",
                "statements": [
                    {
                        "type": "contain",
                        "rule": "all",
                        "values": ["RJ", "SP", "ES", "MG"],
                        "parser": {"dtype": "string"},
                        "min_occurrences": 1,
                        "tolerance_%": 100,
                    }
                ],
            }
        ],
    }
    assert (
        validate(df, against=assertions, raise_exception=False)["items"][0][
            "statements"
        ][0]["report"]["result"]
    ) == "fail"


@pytest.mark.parametrize("backend", list(Backend))
def test_value_error_for_tolerance(backend):
    df = data_reader(
        "tests/statements/test_contain.csv",
        options="tests/statements/test_contain_options.yaml",
        backend=backend,
    )
    assertions = {
        "name": "test_rule_1",
        "items": [
            {
                "scope": "test_maxmin",
                "statements": [
                    {
                        "type": "contain",
                        "rule": "all_and_only",
                        "values": ["RJ", "SP", "ES", "MG"],
                        "parser": {"dtype": "string"},
                        "tolerance_%": 1,
                    }
                ],
            }
        ],
    }
    with pytest.raises(ValueError):
        validate(df, against=assertions, raise_exception=False)
