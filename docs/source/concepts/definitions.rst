===========
Definitions
===========

Here you can find the description about key concepts that you will see when using Deirokay.

Deirokay Options Document
=========================

In order to be able to parse a file, the `deirokay.data_reader` method should receive specifications about how to interpret each column of your data, as well as source-wise parameters, such as file encoding or column separator (in case of `.csv` files). 
Those specifications, called *Deirokay Options document*, can be expressed in form of a JSON/YAML file or a Python object.
To specify each of your columns, you must declare which Deirokay DType should be used, including additional parameters regarding the datatype you chose.

The block of code below depicts an example of options document in JSON format.
The only required parameter for this document is the `columns` key, which contains the specifications for each column.
In current version of Deirokay, all other parameters besides `columns` are transparently passed to the respective Pandas reader, based on the data file extension or source.

.. code-block:: json

    {
        "sep": ",",
        "encoding": "utf-8",
        "columns": {
            "id": {
                "dtype": "integer",
                "thousand_sep": ","
            },
            "customer_name": {
                "dtype": "string"
            },
            "age": {
                "dtype": "integer"
            },
            "revenue": {
                "dtype": "decimal",
                "decimal_places": 2,
                "decimal_sep": ".",
                "thousand_sep": ","
            }
        }
    }

Validation Document
===================

The *Validation Document* gathers all constraints, expectations and business rules that you want to test and validate against your data.

.. code-block:: json

  {
    "name": "CUSTOMERS",
    "description": "Client's data",
    "items": [
      {
        "scope": "customer_name",
        "statements": [
          {
            "type": "row_count",
            "distinct": true,
            "min": 1000
          },
          {
            "type": "unique"
          }
        ]
      },
      {
        "scope": [
          "age",
          "id"
        ],
        "statements": [
          {
            "type": "not_null"
          }
        ]
      }
    ]
  }

The fields that compose such a document are presented below:

Name & Description
------------------

Every validation document starts with the `name` of the dataset that will work on followed by an optional `description`.
Be sure the `name` field does not contain reserved characters if you want your validation logs to be saved to a local folder or S3 bucket.

Validation Items
----------------

Another field in the Validation Document is `items`, which contains a list of `Validation Items`.
A validation item is composed by a `scope` and a `statements` list.
The `statements` are the actual validation rules, which will be applied to the given `scope`.
The `scope` defines a column or a list of columns to be validated.

To get dive into *Deirokay Statements*, you can find more at 
:doc:`Statements <statements>`.


Validation Result
=================

At the end of the data validation, Deirokay create meaningful logs that reflect your validation document. A validation report is attached to each statement, containing its validation result (either `pass` or `fail`) and useful statistics about the analysed scope. 

In the code below, we can see an example of a validation document:

.. code-block:: json

    {
        "name": "CUSTOMERS",
        "description": "Client's data",
        "items": [
            {
                "scope": "transaction_id",
                "statements": [
                    {
                        "type": "not_null",
                        "at_least_%": 100.0,
                        "severity": 1
                    }
                ]
            }
        ]
    }

which generates the following validation report:

.. code-block:: json

    {
        "name": "CUSTOMERS",
        "description": "Client's data",
        "items": [
            {
                "scope": "transaction_id",
                "statements": [
                    {
                        "type": "not_null",
                        "at_least_%": 100.0,
                        "severity": 1,
                        "report": {
                            "detail": {
                                "null_rows": 0,
                                "null_rows_%": 0,
                                "not_null_rows": 830400,
                                "not_null_rows_%": 100
                            },
                            "result": "pass"
                        }

                    }
                ]
            }
        ]
    }

Profiling
=========
A native feature of Deirokay is the ability to generate a validation document from a given template DataFrame.
Once you have correctly use `deirokay.data_reader` to parse your data into a DataFrame, you may use `deirokay.profile` to quickly create a first version of your validation document. 

.. code-block:: python

    from deirokay import data_reader, profile
    from deirokay.enums import Backend

    df = data_reader('file.csv', options='options.json', backend=Backend.PANDAS)

    profile(df, 'CUSTOMERS', save_to='validation_doc.json')


You should get a `validation_doc.json` file at the end of the process, containing a bunch of valid statements about your data.
The results of this function should be used only as a draft for a validation document 
or as a means to quickly launch a first version with minimum efforts. 
It is up to you to modify it and enrich it with your own rules.

By default, this function receives an DataFrame previously parsed using `data_reader`, a  name for the validation document and optionally a path where to save the document to. 
The `profile` method also returns the same document as a Python *dict* object.
