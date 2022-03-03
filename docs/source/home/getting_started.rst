===============
Getting started
===============

In this getting started, you will pass through some of the basic cases where
Deirokay can make your life easer when you are looking for a data quality tool
and this steps consider that you already had installed the Deirokay. 

At the end you will be familiar with the most features that comes with Deirokay, and
if you want more, you can find at the 'How-to' module that contains jupyter notebooks
to show the way in some specific jobs. 


Starts the data quality process
===============================

In any data-driven company, there comes a time when data processing and 
validation becomes a bottleneck for data products. So in many cases the company 
will have a lot of data sources like Databases, File System or Object storage, data that 
comes from another process and many others. Now let's suppose that all this data are gather
together in this *example.csv*. AS you can see below, there is no standard 

.. list-table:: example.csv
   :header-rows: 1

   * - Name
     - Age
     - is_married

   * - john
     - 55
     - True

   * - mariah
     - 44
     - 

   * - carl
     - 
     - False


It's important to notice that the Data validation is a process need in data pipelines,
so it could be possibly to guarantee that all data is correct and free of weird problems
that will lead to errors in next steps of your company.

Some hight validations that could be done
=========================================

Let's consider that you already have download the example.csv and you are working in your
code with pandas. A known issue when dealing with `pandas` is that some of the
data types can be misrepresented as another dtypes as it 's show below:

.. code-block:: python

    >>> import pandas
    >>> pandas.read_csv('example.csv')
    >>>    name    age     is_married
    >>> 0   john    55.0    True
    >>> 1   mariah  44.0    NaN
    >>> 2   carl    NaN     False
    >>> pandas.read_csv('example.csv').dtypes
    >>> name           object
    >>> age           float64
    >>> is_married     object
    >>> dtype: object

Even though that strings are correctly parsed, just look how it goes to integer column that become float column
just because have in it an null cell. If you ever write the file back to your hard disk, you'll get
the following content:

.. list-table:: output.csv
   :header-rows: 1

   * - Name
     - Age
     - is_married

   * - john
     - 55.0
     - True

   * - mariah
     - 44.0
     - 

   * - carl
     - 
     - False

So depending on the dataset that you are working with, you will see different problems like time formats wrongs,
dates, strings and many more. The next session will help you with this.

Before the validation
=====================

After you just look over your data, you saw that will need some changes before its validation. So to go on with
the data quality process, you will use the feature that Deirokay has to deal with some wrong dtypes, to after that,
apply the validation on your data.

So to begin this step, you will need to use de Data Reader feature and with that you will need to pass as a parameters
the path to your dataset file and an option document (which is explained in the concepts section in more detail) that
is an file (.json or .yaml) that contains the specification to analyse the dataset, like what is the separator, or the 
type of encryption its hold and to which column you will have its name, dtype and other options to control the data. 
Below you see an option document:

.. code-block:: json

    {
        “sep”: “|” ,
        “encryption”: “iso-8859-1”,
        “columns”: [
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
        ]
    }

To be able to use this option document you just need to import from Deirokay the DataReader, and will get a
pandas dataframe that doesn't have the initial problems:

.. code-block:: python

  >>> from deirokay import data_reader
  >>> data_reader('example.csv', options='options.json')
  >>>     name   age  is_married
  >>>0    john    55        True
  >>>1    mariah  44        <NA>
  >>>2    carl    <NA>      False
  >>>pandas.read_csv('example.csv').dtypes
  >>>name           object
  >>>age           float64
  >>>is_married     object
  >>>dtype: object

It is good to point out that the `options` argument also accepts `dict` objects directly.
When parsing your file, you may also provide a set of different arguments, which varies in function
of the data types. When passing Deirokay file options as `dict`, you may optionally import the 
available data types from the `deirokay.enums.DTypes` enumeration class.

Making the validation process to work
=====================================

The next step, after you use DataReader is to use the validation document to apply some of the 
statements you want against your data to determine whether it proves to be Right/True or Wrong/False. A Statement is
always evaluated against a scope, i.e., a column or a set of columns. Below you can see the 'assertions.json', 
an example of validation document:

.. code-block:: json

  {
    "name": "example",
    "descripiton": "just a statement test",
    "items": {
      "scope":"name",
         "statements":[
            {
               "type":"row_count",
               "distinct":true,
               "min":1000
            },
            {
               "type":"unique"
            }
         ]
      },
      {
         "scope": "age",
         "statements":[
            {
               "type":"not_null"
            }
         ]
      },
      {
        "scope": "is_married",
        "statements": [
          {
            "type": "contain",
            "severity": 1,
            "True"
          }
        ]
      }
    }
  }

Finale to test your dataset against the validation document, you must import the feature validate
and apply over

.. code-block:: python

  >>> from deirokay import data_reader, validate
  >>> data_reader('example.csv', options='options.json')
  >>>     name   age  is_married
  >>> 0    john    55        True
  >>> 1    mariah  44        <NA>
  >>> 2    carl    <NA>      False
  >>> validation_result_document = validate(df,
                                      against='assertions.json',
                                      raise_exception=False)

The resulting validation document will present the reports for each
statement, as well as its final result: `pass` or `fail`. You may
probably want to save your validation result document by passing a path
to a folder (local or in S3) as `save_to` argument to `validate`. 
By default, the validation result document will be saved in the same file
format as the original validation document (you may specify another
format -- either `json` or `yaml` -- in the `save_format` argument).

Here is an example of validation result document:

``` JSON
{
  "name": "validate_example",
  "description": "An optional field to provide further textual information",
  "items": [
    {
      "scope": [
        "name"
      ],
      "statements": [
        {
          "type": "unique",
          "at_least_%": 90,
          "report": {
            "detail": {
              "unique_rows": 1500,
              "unique_rows_%": 99
            },
            "result": "pass"
          }
        },
        {
          "type": "not_null",
          "at_least_%": 95,
          "report": {
            "detail": {
              "null_rows": 0,
              "null_rows_%": 0,
              "not_null_rows": 1500,
              "not_null_rows_%": 100
            },
            "result": "pass"
          }
        }
      ]
    }
  ]
}
```

