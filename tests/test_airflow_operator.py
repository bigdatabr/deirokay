import pytest

from deirokay.airflow import DeirokayOperator
from deirokay.exceptions import ValidationError


def test_deirokay_operator():
    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        deirokay_options_json='tests/options.json',
        deirokay_assertions_json='tests/assertions.json',
        pandas_kwargs=dict(encoding='iso-8859-1', sep=';')
    )

    with pytest.raises(ValidationError):
        operator.execute({})
