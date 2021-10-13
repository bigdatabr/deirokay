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
