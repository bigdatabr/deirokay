========
Concepts
========

Here you can find the description about key concepts that you will see 
when you are using Deirokay.


DTypes and Threaders
====================

For Deirokay, like in any other application, data types are important 
concept. So when you are using DataReader or Data validation, there is 
some data types that you can use to work, and here it is with there 
alias: 

.. list-table:: 
   :header-rows: 1

   * - DTypes
     - String-like alias
     - Supported Arguments
     - Default
     - Argument Description

   * - All DTypes
     -
     - nullable
     - True
     - Values can be null

   * - All DTypes
     -
     - unique
     - False
     - Values should be unique

   * - All DTypes
     -
     - rename
     - None
     - Rename column

   * - INTEGER
     - 'integer'
     - thousand_sep
     - None
     - Thousand separator (e.g., "1,988")

   * - FLOAT
     - 'float'
     - thousand_sep
     - None
     - Thousand separator (e.g., "1,988")

   * - FLOAT
     - 'float'
     - decimal_sep
     - '.'
     - Decimal separator (e.g., "3.14")

   * - DECIMAL
     - 'decimal'
     - decimal_sep
     - '.'
     - Decimal separator (e.g., "3.14")

   * - DECIMAL
     - 'decimal'
     - thousand_sep
     - None
     - Thousand separator (e.g., "1,988")

   * - DECIMAL
     - 'decimal'
     - decimal_places
     - None
     - Decimal places (e.g., 2 for "1.25")

   * - BOOLEAN
     - 'boolean'
     - truthies
     - ['true', 'True']
     - Values taken as True

   * - BOOLEAN
     - 'boolean'
     - falsies
     - ['false', 'False']
     - Values taken as False

   * - BOOLEAN
     - 'boolean'
     - ignore_case
     - False
     - Ignore case when evaluating True/False

   * - BOOLEAN
     - 'boolean'
     - default_value
     - None
     - Value to use if not truthy nor falsy

   * - DATETIME
     - 'datetime'
     - format
     - '%Y-%m-%d %H:%M:%S'
     - Date Time format

   * - TIME
     - 'time'
     - format
     - '%H:%M:%S'
     - Time format

   * - DATE
     - 'date'
     - format
     - '%Y-%m-%d'
     - Date format

   * - STRING
     - 'string'
     - treat_null_as
     - None
     - Value to replace when null (e.g., "")

Other concept to keep an eye on is Threaders. When you use the
DataReader and Data validation, Deirokay use some of class to deal with
your needs when it is about threat data, like NumericTreater, 
BooleanTreater, FloatTreater, DecimalTreater, IntegerTreater, 
DateTreater , TimeTreater, StringTreater. In the future, in case you 
need new threaders you just inherits from the bases classes and build
your own theater.


Options Document
================

The process to treated an dataframe, deirokay uses the Data Reader 
module that works with the options documents. This document holds a 
parameters like 'sep'( separator), 'encryption' and 'columns'. This 
last one specifies your parsing options that will be applied in choice 
features. You can use the DTypes to work in your described column, 
thousand separator, decimal places and format data. At the end of this
process your features will be as you would excepts to. 

.. code-block:: json

    {
        "sep": "|",
        "encryption": "iso-8859-1",
        "columns": [
            “customer_name”: {
                “dtype”: “string”
            },
            “age”: {
                “dtype”: “integer”,
                “thousand_sep”: “.”
            },
            “id”: {
                “dtype”: “integer”,
                “thousand_sep”: “.”
            },
            “revenue”: {
                “dtype”: “decimal”,
                “decimal_places”: 2,
                “decimal_sep”: “,”,
                “thousand_sep”: “.”
            }
        ]
    }

Validation Document
===================
In the validation process, the most important thing to know is that 
the validation document can be write for every step in the pipeline 
process to validate the dataframe or file. Here you find the parts that 
compose the document that validates your data.

1. Name & Descripiton
---------------------

Every validation document starts with the name of the dataset that will 
work on and followed by an descripiton to help that everyone can 
understand.

.. code-block:: json

    {
        "name": "CUSTOMERS",
        "description": "Client's data"
    }

2. Validation items
---------------------

Anther topic in the Validation Document is the validation items, 
that are composed by the validation statements and validation scope. 
The last one defines which columns will be analysed and you can pass 
one or more groups of columns. Within the scope you can pass an alias 
to the statement. To get dive into it, you can find more at 
.. _Statements. Last but not least, the validation statements are need 
to you specifies what parameters you need, like the 'type', 'distinct', 
'min', 'severity', 'at_least', 'max', when you work through the 
columns of your dataset.

.. code-block:: json
    
    {
   "name":"CUSTOMERS",
   "descripiton":"Client's data",
   "items":[
      {
         "scope":"customer_name",
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
         "scope":[
            "age",
            "id"
         ],
         "statements":[
            {
               "type":"not_null"
            }
         ]
      }
   ]
}


Validation Document Result
==========================
At the end of your data validation, deirokay can organize an output 
document(.json/.yaml) that reflects your validation document and have a 
plus of the report statement,this shows if your statements pass or not 
in relational to your analyses described in the validation items in 
detail. See below an example

.. code-block:: json

    {
        "name": "CUSTOMERS",
        "description": "Client's data",
        "items": [
            {
                “scope”: "NUM_TRANSACAO01",
                "alias": "test"
                “statements”: 
                    {
                        “type”: "not_null",
                        "at_least_%": 100.0,
                        “severity": 1
                    }
            }
        ]
    }

And the result wil be:

.. code-block:: json

    {
        "name": "CUSTOMERS",
        "description": "Client's data",
        "items": [
            {
                “scope”: "NUM_TRANSACAO01",
                "alias": "test",
                “statements”: 
                    {
                        “type”: "not_null",
                        "at_least_%": 100.0,
                        “severity": 1,
                        "report": {
                            "detail": {
                                "num_rows": 0,
                                "num_rows_%": 0,
                                "not_num_rows": 830400,
                                "not_num_rows_%": 100
                            },
                            "result": "pass"
                        }

                    }
            }
        ]
    }

Profiling
=========
An extra function that comes with Deirokay is you can be able to 
generate a validation document from a given template DataFrame for 
builtin Deirokay statements. See the exempla below:

.. code-block:: python

    from deirokay import data_reader, validate, profile
    from datetime import datetime

    df = data_reader('file.csv', options='options.json')

    profile(df, 'CUSTOMERS', save_to='validation_doc.json')

    ### Later

    validate(
    df, assertions='validation_doc.json', save_to='.'
    )

And you wil get an "validation_doc.json" at the end of the process, similar to this:

.. code-block:: json

    {
      "name":"CUSTOMERS",
      "description":"Autogenerated…",
      "items":[
          {
            "scope":"customer_name",
            "statements":[
                {
                  "type":"unique",
                  "at_least_%":95.25
                },
                {
                  "type":"not_null",
                  "at_least_%":95.25
                }
            ]
          }
      ]
  }

This function should be used only as a draft for a validation document 
or as a means to quickly launch a first version with minimum efforts.By 
default, this function receives an DataFrame (that as ideally parsed
with DataReader), an document name(string) that represents the 
validation document name and the path where wil save it( like local or 
S3). At the end, auto-generated validation document as Python 'dict'.
returns an. If no path are passed, no document will be save by default.

.. include:: contents.rst
