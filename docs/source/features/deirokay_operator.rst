Deirokay Airflow Operator
=========================

Deirokay has its own Airflow Operator, which you can import to your DAG
to validate your data.


.. code-block:: python

    from datetime import datetime

    from airflow.models import DAG
    from deirokay.airflow import DeirokayOperator


    dag = DAG(dag_id='data-validation',
              schedule_interval='@daily',
              default_args={
                  'owner': 'airflow',
                  'start_date': datetime(2021, 3, 2),
                  'trigger_rule': 'none_failed',
              })

    operator = DeirokayOperator(task_id='deirokay-validate',
                                data='tests/transactions_sample.csv',
                                options='tests/options.json',
                                against='tests/assertions.json',
                                dag=dag)


The `DeirokayOperator` class can be imported from :ref:`deirokay.airflow.DeirokayOperator<deirokay.airflow.deirokay\\_operator.DeirokayOperator>`.


.. todo::

    - Details about DeirokayOperator `soft_fail_level` and `hard_fail_level`.
    - Usage of `trigger_rule: 'none_failed'` in `default_args`.
