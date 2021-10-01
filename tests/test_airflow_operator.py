from deirokay.airflow import DeirokayOperator


def test_deirokay_operator():
    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        deirokay_options_json='tests/options.json',
        deirokay_assertions_json='tests/assertions.json',
    )

    operator.execute({})
