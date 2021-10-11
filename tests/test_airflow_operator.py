from deirokay.airflow import DeirokayOperator


def test_deirokay_operator():
    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        options='tests/options.json',
        against='tests/assertions.json',
    )

    operator.execute({})
