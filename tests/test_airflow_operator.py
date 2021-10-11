from deirokay.airflow import DeirokayOperator


def test_deirokay_operator():
    operator = DeirokayOperator(
        task_id='deirokay_validate',
        path_to_file='tests/transactions_sample.csv',
        deirokay_options='tests/options.json',
        deirokay_assertions='tests/assertions.json',
    )

    operator.execute({})
