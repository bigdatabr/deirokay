==========
Getting started
==========

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


Making Statements about your data
==============================

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