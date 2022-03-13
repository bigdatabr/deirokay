Statement Severity Level
========================

An optional parameter for your statements is the `severity` level,
whose value is typically an integer from 1 to 5 (although nothing
prevents you from using any other integer value).
The :ref:`deirokay.enums.SeverityLevel` enumeration describes some named
levels: `MINIMAL` (1), `WARNING` (3) and `CRITICAL` (5), but you may
use any other integer in your Validation Documents instead.
When not declared in your validation document, the default statement
severity level is set to 5.

The `severity` parameter is particularly useful in two contexts:

- When using `deirokay.validate` function, you may want to set
  `raise_exception` to `True` (default value) and specify a
  `exception_level` (default is `CRITICAL` or 5). This function call will
  raise a `deirokay.exceptions.ValidationError` exception whenever it
  finds a failed statement whose severity level is greater or equal to
  the `exception_level` variable.
  The `level` attribute of `ValidationError` contains the larger severity
  level found in all failed statements.
  Any failed statement whose value is lesser than `exception_level` will
  only raise a warning.
- When validating data in *Apache Airflow* with *DeirokayOperator*, you
  may specify two exception levels: `soft_fail_level` (defaults to
  `MINIMAL` or 1) and `hard_fail_level` (defaults to `CRITICAL` or 5).
  The task is given the `failed` status if any failed statement severity
  matches or exceeds the `hard_fail_level`, and any tasks that may depend
  on its success won't be executed.
  Otherwise, if the severity level for any failed statement only matches
  or exceeds `soft_fail_level`, the task is given the `skipped` status,
  which won't prevent the DAG to keep running.
  If all failed statements have severity levels lesser than
  `soft_fail_level`, only a warning will be logged by the task.
