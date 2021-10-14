# Deirokay

A tool for data profiling and data validation.

Deirokay separates document parsing from validation logic,
so that you can create your statements about your data
without worrying whether or not your file has been properly
parsed.

You can use Deirokay for:
- Data parsing from files;
- Data validation, via Deirokay Statements;
- Data profiling, which generates Deirokay Statements automatically.
You may use them later with new documents to make sure they are still
valid.

To start using Deirokay, install its package and follow
the instructions bellow:


## Installation

Install Deirokay directly from `master` branch typing in your
command line:

`pip install git+http://gitlab.bigdata/bressanmarcos/deirokay`

To include optional dependences for AWS S3, install:

`pip install git+http://gitlab.bigdata/bressanmarcos/deirokay[s3]`

If you want to be in sync with the latest (and possibly unstable) release:

`pip install git+http://gitlab.bigdata/bressanmarcos/deirokay@dev`

## Installation for development

If you wish to contribute for Deirokay development, maybe
you will want to install a more complete set of packages for
testing and to help you in your development.

`pip install git+http://gitlab.bigdata/bressanmarcos/deirokay[dev]`

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
provide a JSON file that explicitly specifies how each column
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

Now, import `Deirokay.data_reader` and pass the JSON file
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

When parsing your file, you may also provide a set of different
arguments, which varies in function of the data types. When
passing Deirokay file options as `dict`, you may also want to
import the available data types from the `deirokay.enums.DTypes`
enumeration class.

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
which can be either a JSON file or a Python dict - and used as
a declarative language to test the properties of your data.

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
from deirokay import data_reader
from deirokay import validate

df = data_reader('file.parquet', options='options.json')
validation_result_document = validate(df,
                                      against='assertions.json',
                                      raise_exception=False)
```

The resulting validation document will present the reports for each
statement, as well as its final result: `pass` or `fail`.

Currently, the following statement types are supported:

Statement Type | Available Arguments
---|---
'unique' | 'at_least_%' | Minimum percentage of unique rows
'not_null' | 'at_least_%' | Minimum percentage of non-null rows
'not_null' | 'at_most_%' | Maximum percentage of non-null rows
'not_null' | 'multicolumn_logic' | Whether to use 'any' or 'all' when evaluating non-null rows over multiple colums
'custom' | 'location' | Location of the custom statement (e.g., "/home/file.py::MyStatement")

The following section illustrates how to create and use `custom` type
statements.

### Creating Custom Statements

Deirokay is designed to be broadly extensible. Even if your set
of rules about your data is not in the bultin set of Deirokay
statements, you can still subclass the `BaseStatement` class and
implement your own statements. If you believe your statement will be
useful for others, we encourage you to propose it as a Merge Request
so that it can become a builtin statement.

A custom Statement should override at least two methods: `report`
and `result`. The `report` method presents statistics and
measurements that may be used by the `result` method. The report is
attached to the Validation Result Document in order to compose a
detailed list of facts about your data. The `result` method receives
the report generated by `report` and returns either `True` (to signal
success) or `False` (to signal that the statement is false).
 

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
        # from `self.options`. If they were templated arguments, now they
        # should have been already rendered.
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

Currently, you can pass a local path or an S3 key:
- `/home/ubuntu/my_module.py::MyStatementClass`
- `s3://my-bucket/my_statements/module_of_statements.py::Stmt` (make sure you have boto3 installed)

## Data Profiling: Auto-generated Validation Document

You may generate a basic validation document by consuming a sample file.
It is recommended that you review the generated document and
supplement it with additional statements.


``` python
from deirokay import data_reader, profile


df = data_reader(
    'tests/transactions_sample.csv',
    options='tests/options.json'
)

validation_document = profile(df,
                              document_name='my-document-name',
                              save_to='my-validation-document')
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
                            path_to_file='tests/transactions_sample.csv',
                            options='tests/options.json',
                            against='tests/assertions.json',
                            dag=dag)
```
