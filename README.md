# Deirokay
`master`:
![pipeline status](http://gitlab.bigdata/data-engineers/deirokay/badges/master/pipeline.svg)
![test coverage](http://gitlab.bigdata/data-engineers/deirokay/badges/master/coverage.svg)
![docstr-coverage](http://gitlab.bigdata/data-engineers/deirokay/-/jobs/artifacts/master/raw/badge.svg?job=docstr-coverage)

`dev`:
![pipeline status](http://gitlab.bigdata/data-engineers/deirokay/badges/dev/pipeline.svg)
![test coverage](http://gitlab.bigdata/data-engineers/deirokay/badges/dev/coverage.svg)
![docstr-coverage](http://gitlab.bigdata/data-engineers/deirokay/-/jobs/artifacts/dev/raw/badge.svg?job=docstr-coverage)


Deirokay (*dejɾo'kaj*) is a tool for data profiling and data validation.

Deirokay separates document parsing from validation logic,
so that you can create your statements about your data
without worrying whether or not your file has been properly
parsed.

You can use Deirokay for:
- Data parsing from files (CSV, parquet, excel, or any other
pandas-compatible format);
- Data validation, via *Deirokay Statements*;
- Data profiling, which generates Deirokay Statements automatically
based on an existing file. You may use these statements later against
new documents to make sure the validation still holds for new data.

To start using Deirokay, install its package and follow
the instructions bellow.


## Installation

Install Deirokay directly from `master` branch typing in your
command line:

`pip install git+http://gitlab.bigdata/data-engineers/deirokay`

To include optional dependences for AWS S3, install:

`pip install git+http://gitlab.bigdata/data-engineers/deirokay[s3]`

If you want to be in sync with the latest (and possibly unstable) release:

`pip install git+http://gitlab.bigdata/data-engineers/deirokay@dev`

## Installation for development

If you wish to contribute for Deirokay development, maybe
you will want to install a more complete set of packages for
testing and to help you in your development.

`pip install git+http://gitlab.bigdata/data-engineers/deirokay[dev]`

## API Reference

Please, [read the docs](http://data-engineers.docs.bigdata/deirokay).

## Getting started

Suppose the following CSV file:

name | age | is_married
---|---|---
john | 55 | true
mariah | 44 |
carl | | false

A known issue when dealing with `pandas` is that some of the
datatypes can be misrepresented as another dtypes. See the
result when reading this file with `pandas`:
```
>>> import pandas
>>> pandas.read_csv('file.csv')
     name   age is_married
0    john  55.0       True
1  mariah  44.0        NaN
2    carl   NaN      False
>>> pandas.read_csv('file.csv').dtypes
name           object
age           float64
is_married     object
dtype: object
```

Although strings are correctly parsed, integers become floats
when null cell are present in the same column.
If you ever write the file back to your hard disk, you'll get
the following content:

name | age | is_married
---|---|---
john | 55.0 | True
mariah | 44.0 |
carl | | False


Now all your integers became floats!

To prevent this unexpected behavior, Deirokay requires you to
provide a JSON (or YAML) file that explicitly specifies how each column
should be parsed:

``` JSON
{
    "columns": {
        "name": {
            "dtype": "string",
            "nullable": false,
            "unique": true
        },
        "age": {
            "dtype": "integer",
            "nullable": true,
            "unique": false
        },
        "is_married": {
            "dtype": "boolean",
            "nullable": true,
            "unique": false
        }
    }
}
```

Now, import `Deirokay.data_reader` and pass the JSON/YAML file
as argument:
```
>>> from deirokay import data_reader
>>> data_reader('file.csv', options='options.json')
     name   age  is_married
0    john    55        True
1  mariah    44        <NA>
2    carl  <NA>       False
>>> data_reader('file.csv', options='options.json').dtypes
name           object
age             Int64
is_married    boolean
dtype: object
```

The `options` argument also accepts `dict` objects directly.
When parsing your file, you may also provide a set of different
arguments, which varies in function of the data types. When
passing Deirokay file options as `dict`, you may optionally import
the available data types from the `deirokay.enums.DTypes` enumeration
class.

Below you will find a list of current data types and their
supported arguments.

DTypes     | String-like alias | Supported Arguments | Default             | Argument Description
:----------|:-----------------:|:-------------------:|:-------------------:|:--------------------------------------
All DTypes | -                 | nullable            | True                | Values can be null
All DTypes | -                 | unique              | False               | Values shoud be unique
All DTypes | -                 | rename              | None                | Rename column
INTEGER    | 'integer'         | thousand_sep        | None                | Thousand separator (e.g., "1,988")
FLOAT      | 'float'           | thousand_sep        | None                | Thousand separator (e.g., "1,988")
FLOAT      | 'float'           | decimal_sep         | '.'                 | Decimal separator (e.g., "3.14")
DECIMAL    | 'decimal'         | decimal_sep         | '.'                 | Decimal separator (e.g., "3.14")
DECIMAL    | 'decimal'         | thousand_sep        | None                | Thousand separator (e.g., "1,988")
DECIMAL    | 'decimal'         | decimal_places      | None                | Decimal places (e.g., 2 for "1.25")
BOOLEAN    | 'boolean'         | truthies            | ['true', 'True']    | Values taken as True
BOOLEAN    | 'boolean'         | falsies             | ['false', 'False']  | Values taken as False
BOOLEAN    | 'boolean'         | ignore_case         | False               | Ignore case when evaluating True/False
BOOLEAN    | 'boolean'         | default_value       | None                | Value to use if not truthy nor falsy
DATETIME   | 'datetime'        | format              | '%Y-%m-%d %H:%M:%S' | Date Time format
TIME       | 'time'            | format              | '%H:%M:%S'          | Time format
DATE       | 'date'            | format              | '%Y-%m-%d'          | Date format
STRING     | 'string'          | treat_null_as       | None                | Value to replace when null (e.g., "")

Along with the specification for the columns, Deirokay options also
support specification of parameters to properly open the file.
These extra parameters are passed to `pandas.read_*` methods when
reading it. For instance, you may want to specify the separator and
the encoding for your CSV file:
``` JSON
{
    "sep": ";",
    "encoding": "iso-8859-1",
    "columns": {
        "customer_id": {
            "dtype": "integer",
            "nullable": false,
            "thousand_sep": ".",
            "unique": false
        },
        "transaction_date": {
            "dtype": "datetime",
            "format": "%Y%m%d"
        },
        "transaction_id": {
            "dtype": "integer",
            "nullable": false,
            "thousand_sep": ".",
            "unique": false
        }
    }
} 
```

Except for `columns`, all other parameters from Deirokay options are
passed to `pandas.read_*` methods when opening the file.


### Making Statements about your data

The main entity in Deirokay is called Statement. A Statement is
a form of test that is executed against your data to determine
whether it proves to be Right/True or Wrong/False. A Statement is
always evaluated against a scope, i.e., a column or a set of columns.

A set of Statements are packed together in a Validation Document -
which can be either a JSON file, an YAML file or a Python dict - and
used as a declarative language to test the properties of your data.

Here is an example of Validation Document in JSON format:
``` JSON
{
    "name": "VENDAS",
    "description": "An optional field to provide further textual information",
    "items": [
        {
            "scope": [
                "WERKS01",
                "DT_OPERACAO01"
            ],
            "statements": [
                {
                    "type": "unique",
                    "at_least_%": 90.0
                },
                {
                    "type": "not_null",
                    "at_least_%": 95.0
                }
            ]
        }
    ]
}
```

To test your data against this document, import the `deirokay.validate`
method and call it following the example below:
``` python
from deirokay import data_reader, validate

df = data_reader('file.parquet', options='options.json')
validation_result_document = validate(df,
                                      against='assertions.json',
                                      raise_exception=False)
```

The resulting validation document will present the reports for each
statement, as well as its final result: `pass` or `fail`. You may
probably want to save your validation result document by passing a path
to a folder (local or in S3) as `save_to` argument to `validate`. The
results are saved in a subfolder named after your validation document
name, and the current datetime (possibly overridden by `current_date`
argument) is used as the file name.
By default, the validation result document will be saved in the same file
format as the original validation document (you may specify another
format -- either `json` or `yaml` -- in the `save_format` argument).

Here is an example of validation result document:

``` JSON
{
    "name": "VENDAS",
    "description": "An optional field to provide further textual information",
    "items": [
        {
            "scope": [
                "WERKS01",
                "DT_OPERACAO01"
            ],
            "statements": [
                {
                    "type": "unique",
                    "at_least_%": 90.0,
                    "report": {
                        "detail": {
                            "unique_rows": 1500,
                            "unique_rows_%": 99.0
                        },
                        "result": "pass"
                    }
                },
                {
                    "type": "not_null",
                    "at_least_%": 95.0,
                    "report": {
                        "detail": {
                            "null_rows": 0,
                            "null_rows_%": 0.0,
                            "not_null_rows": 1500,
                            "not_null_rows_%": 100.0
                        },
                        "result": "pass"
                    }
                }
            ]
        }
    ]
}
```

These are some of the statement types currently supported by Deirokay:

Statement Type | Available Arguments
---|---
'unique' | 'at_least_%' | Minimum percentage of unique rows
'not_null' | 'at_least_%' | Minimum percentage of non-null rows
'not_null' | 'at_most_%' | Maximum percentage of non-null rows
'not_null' | 'multicolumn_logic' | Whether to use 'any' or 'all' when evaluating non-null rows over multiple colums
'custom' | 'location' | Location of the custom statement (e.g., "/home/file.py::MyStatement")

The following section illustrates how to create and use `custom` type
statements.

### Statement Severity Level

An optional parameter for your statements is the `severity` level,
whose value is typically an integer from 1 to 5 (although nothing
prevents you to use any other integer).
The `deirokay.enums.SeverityLevel` enumeration describes some named
levels: `MINIMAL` (1), `WARNING` (3) and `CRITICAL` (5), but you may
use any other integer in your Validation Documents instead.
When not declared, the default statement severity level is set to 5.

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

## Creating Custom Statements

Deirokay is designed to be broadly extensible. Even if your set
of rules about your data is not in the bultin set of Deirokay
statements, you can still subclass the `BaseStatement` class and
implement your own statements. If you believe your statement will be
useful for other users, we encourage you to propose it as a
Merge Request so that it can become a builtin statement in a
future release.

A custom Statement should override at least two methods from
`BaseStatement`: `report` and `result`.
The `report` method presents statistics and
measurements that may be used by the `result` method. The report is
attached to the Validation Result Document in order to compose a
detailed list of facts about your data. The `result` method receives
the report generated by `report` and returns either `True` (to signal
success) or `False` (to signal that the statement is invalid).
 

The code below shows an example of a custom statement:
``` python
from deirokay.statements import BaseStatement


class ThereAreValuesGreaterThanX(BaseStatement):
    # Give your statement class a name (only for completeness,
    # its name is only useful when proposing it in a Merge Request)
    name = 'there_are_values_greater_than_x'
    # Declare which parameters are valid for this statement
    expected_parameters = ['x']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # All the arguments necessary for the statement are collected
        # from `self.options`. If they were templated arguments, they
        # should have already been rendered and you may transparently
        # use their final value in `report` and `result` methods.
        self.x = self.options.get('x')

    def report(self, df) -> dict:
        """
            Report statistics and facts about the data.
        """
        bools = df > self.x
        report = {
            'values_greater_than_x': list(bools[bools.all(axis=1)].index)
        }
        return report

    def result(self, report: dict) -> bool:
        """
            Use metrics from the report to indicate either success
            (True) or failure (False)
        """
        return len(report.get('values_greater_than_x')) > 0
```

The following Validation Document shows how to use your custom
Statement for a validation process:
``` JSON
{
        "name": "VENDAS",
        "description": "Validation using custom statement",
        "items": [
            {
                "scope": "NUM_TRANSACAO01",
                "statements": [
                    {
                        "type": "custom",
                        "location": "/home/custom_statement.py::"
                                    "ThereAreValuesGreaterThanX",
                        "x": 2
                    }
                ]
            }
        ]
    }
```

Besides the parameters necessary for your custom statement (`"x": 2` 
in the example above) and the `custom` statement type, you should pass
a `location` parameter that instructs Deirokay how to find your
statement class. There is not need for the module file to be in
current directory: your class will be magically imported by Deirokay
and used during validation process. 

The `location` parameter must follow the pattern 
`path_to_module::class_name`.

Currently, you can pass either a local path or an S3 key:
- `/home/ubuntu/my_module.py::MyStatementClass`
- `s3://my-bucket/my_statements/module_of_statements.py::Stmt`
(make sure you have `boto3` installed)

## Jinja Templating and Statements based on Past Validation Data

Some validation statements may present dynamic behaviour, maybe
folowing a natural uptrend or downtrend movement of your data. Suppose
you expect the number of rows of your data file to possibly fluctuate
+/- 3% around a 7-day moving average. Deirokay allows you to refer to
past validation data by means of a special function called `series`.

To use it, replace a static value for your statement parameter by a
templated argument, such as the following:

``` yaml
name: VENDAS
items:
- scope:
  - WERKS01
  - PROD_VENDA
  #  When the scope has more than one column or has special characters,
  #  you should provide an `alias` string to refer to this item.
  alias: werks_prod
  statements:
  - type: row_count
    min: '{{ 0.97 * (series("VENDAS", 7).werks_prod.row_count.rows.mean()
      |default(19, true)) }}'
    max: '{{ 1.03 * (series("VENDAS", 7).werks_prod.row_count.rows.mean()
      |default(21, true)) }}'
```

The YAML validation document above (could be JSON) presents some new
features. A templated Jinja argument is identified by a pair of double
braces (`{{ }}`) surrounding its content. Deirokay has a special
callable named `series` that you can use to retrieve past data as a
`pandas.Series` object.
When declared, `series` receives a validation document name and a
number of logs to look behind. Next, you should provide a path to a
statement report value, following the sequence:
scope (possibly aliased) => statement type name => statement result
metric name. This returns a `pandas.Series` object you can take any
calculation from (`mean`, `min`, `max`, etc.).

To illustrate this path, take the templated argument from the YAML
document above as an example:

``` python
series("VENDAS", 7).werks_prod.row_count.rows.mean()
```
- `series("VENDAS", 7)`: Retrieve the 7 last validation logs for
"VENDAS";
- `werks_prod`: Consider the `werks_prod`-aliased item (could be the
scope value itself if it is a string for a single column name);
- `row_count`: Use statistics from `row_count` statement;
- `rows`: Use the `rows` metric reported by the selected statement
(you should know the statistics reported by your statement).
- `mean()`: Since the previous object is already a pandas Series, this
call gets its mean value.

When your validation results have no history, this call returns `None`.
In Jinja, you may provide a default value to replace a null variable
by using `|default(<default value>, true)`.

Finally, here is an example of validation result log. Notice that the
templates are already rendered before being saved:

``` yaml
name: VENDAS
items:
- scope:
  - WERKS01
  - PROD_VENDA
  alias: werks_prod
  statements:
  - type: row_count
    min: 1490.0
    max: 1510.0
    report:
      detail:
        rows: 1500
      result: pass
```

## Data Profiling: Auto-generated Validation Document

You may generate a basic validation document by consuming a sample file.
It is recommended that you review the generated document and
supplement it with additional statements. The document can be saved in
either JSON or YAML format, depending on the extension of the file path
that you passed to optional `save_to` argument. You may also retrieve
the `dict` document from the method's return.


``` python
from deirokay import data_reader, profile


df = data_reader(
    'tests/transactions_sample.csv',
    options='tests/options.json'
)

validation_document = profile(df,
                              document_name='my-document-name',
                              save_to='my-validation-document.json')
```

## Deirokay Airflow Operator

Deirokay has its own Airflow Operator, which you can import to your DAG
to validate your data.


``` python
from datetime import datetime

from airflow.models import DAG
from deirokay.airflow import DeirokayOperator


dag = DAG(dag_id='data-validation',
          schedule_interval='@daily',
          default_args={
              'owner': 'airflow',
              'start_date': datetime(2021, 3, 2),
          })

operator = DeirokayOperator(task_id='deirokay-validate',
                            data='tests/transactions_sample.csv',
                            options='tests/options.json',
                            against='tests/assertions.json',
                            dag=dag)
```
