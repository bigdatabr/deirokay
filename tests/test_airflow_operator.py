import pytest
from airflow.exceptions import AirflowSkipException

from deirokay.airflow import DeirokayOperator


def test_deirokay_operator(prepare_history_folder):
    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        options='tests/options.json',
        against='tests/assertions_with_history.json',
        save_to=prepare_history_folder
    )

    operator.execute({'ts_nodash': '20001231T101010'})
    operator.execute({'ts_nodash': '20001231T101011'})
    operator.execute({'ts_nodash': '20001231T101012'})


def test_deirokay_operator_with_severity(prepare_history_folder):

    assertions = {
        "name": "VENDAS",
        "description": "A sample file",
        "items": [
            {
                "scope": [
                    "WERKS01",
                    "PROD_VENDA"
                ],
                "alias": "werks_prod",
                "statements": [
                    {
                        "type": "unique",
                        "at_least_%": 40.0
                    },
                    {
                        "type": "unique",
                        "at_least_%": 90.0,
                        "severity": 1
                    }
                ]
            }
        ]
    }

    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        options='tests/options.json',
        against=assertions,
        save_to=prepare_history_folder
    )

    with pytest.raises(AirflowSkipException):
        operator.execute({'ts_nodash': '20001231T101010'})
