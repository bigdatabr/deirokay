import json

import pytest

from deirokay import data_reader, validate
from deirokay.enums import Backend, SeverityLevel
from deirokay.exceptions import ValidationError
from deirokay.fs import split_s3_path


@pytest.mark.parametrize("backend", list(Backend))
def test_data_invalidation_from_dict(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "name": "VENDAS",
        "items": [
            {
                "scope": ["WERKS01", "DT_OPERACAO01"],
                "statements": [
                    {
                        "type": "unique",
                        "at_least_%": 1.0,
                    }
                ],
            }
        ],
    }

    with pytest.raises(ValidationError):
        validate(df, against=assertions)


@pytest.mark.parametrize("backend", list(Backend))
def test_data_validation_from_yaml(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.yaml", backend=backend
    )

    validate(df, against="tests/assertions.yml")


@pytest.mark.parametrize("backend", list(Backend))
def test_data_validation_from_json(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    validate(df, against="tests/assertions.json")


@pytest.mark.parametrize("backend", list(Backend))
def test_custom_statement(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "name": "VENDAS",
        "items": [
            {
                "scope": "NUM_TRANSACAO01",
                "statements": [
                    {
                        "type": "custom",
                        "location": "tests/custom_statement.py"
                        "::ThereAreValuesGreaterThanX",
                        "x": 2,
                    }
                ],
            }
        ],
    }

    validate(df, against=assertions)


@pytest.fixture
def prepare_s3_custom_statement(require_s3_test_bucket):
    local_path = "tests/custom_statement.py"
    s3_path = f"s3://{require_s3_test_bucket}/custom_statement.py"
    bucket, key = split_s3_path(s3_path)

    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    yield s3_path
    s3.delete_object(Bucket=bucket, Key=key)


@pytest.mark.parametrize("backend", list(Backend))
def test_custom_statement_from_s3(prepare_s3_custom_statement, backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "name": "VENDAS",
        "items": [
            {
                "scope": "NUM_TRANSACAO01",
                "statements": [
                    {
                        "type": "custom",
                        "location": (
                            prepare_s3_custom_statement + "::ThereAreValuesGreaterThanX"
                        ),
                        "x": 2,
                    }
                ],
            }
        ],
    }

    validate(df, against=assertions)


@pytest.mark.parametrize("backend", list(Backend))
def test_data_validation_with_jinja(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "name": "VENDAS",
        "items": [
            {
                "scope": ["WERKS01", "PROD_VENDA"],
                "statements": [
                    {"type": "unique", "at_least_%": "{{ 40.0 }}"},
                    {"type": "not_null", "at_least_%": 95.0},
                    {"type": "row_count", "min": "{{ 18 }}", "max": 22},
                ],
            },
            {
                "scope": "DT_OPERACAO01",
                "statements": [
                    {
                        "type": "contain",
                        "rule": "all_and_only",
                        "values": ["{{ today }}"],
                        "parser": {"dtype": "datetime", "format": "%Y%m%d"},
                    }
                ],
            },
        ],
    }

    validate(df, against=assertions, template={"today": "20210816"})


@pytest.mark.parametrize("backend", list(Backend))
def test_data_validation_with_levels(backend):
    df = data_reader(
        "tests/transactions_sample.csv", options="tests/options.json", backend=backend
    )

    assertions = {
        "name": "VENDAS",
        "items": [
            {
                "scope": ["WERKS01", "PROD_VENDA"],
                "statements": [
                    {
                        "type": "unique",
                        "severity": SeverityLevel.WARNING,
                        "at_least_%": "{{ 100.0 }}",
                    },
                    {
                        "type": "not_null",
                        "at_least_%": 100.0,
                    },
                    {"type": "row_count", "min": "{{ 18 }}", "max": 22},
                ],
            }
        ],
    }

    validate(df, against=assertions)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"level-1a": ["a", "b"]},
            {"level-1a": ["a", "b"]},
        ),
        (
            {"level-1b": ["a", "b", "c"]},
            {"level-1b": ["a", "..."]},
        ),
        (
            {
                "level-2": {"level-2a": ["a", "b"]},
            },
            {
                "level-2": {"level-2a": ["a", "b"]},
            },
        ),
        (
            {
                "level-2": {"level-2b": ["a", "b", "c"]},
            },
            {
                "level-2": {"level-2b": ["a", "..."]},
            },
        ),
        (
            {"sublist": [["a", "b"], ["a", "b", "c"]]},
            {"sublist": [["a", "b"], ["a", "..."]]},
        ),
    ],
)
def test_sane_json_print(monkeypatch, capsys, data, expected):
    import deirokay.validator
    from deirokay.validator import _sane_json_print

    monkeypatch.setattr(deirokay.validator, "_TRUNCATE_OUTPUT_LIMIT", 2)

    _sane_json_print(data)

    captured = capsys.readouterr()
    assert json.dumps(expected, indent=4) in captured.out
    if data != expected:
        assert "was truncated" in captured.out
