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

.. code-block:: console

    >>>import pandas
    >>>pandas.read_csv('example.csv')
    >>>    name    age     is_married
    >>>0   john    55.0    True
    >>>1   mariah  44.0    NaN
    >>>2   carl    NaN     False
    >>>pandas.read_csv('example.csv').dtypes
    >>>name           object
    >>>age           float64
    >>>is_married     object
    >>>dtype: object

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
        “sep”: “|”,
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

.. code-block:: console

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


Making the validation process work
==================================
    - E aí então insere validation documents para expressar as validações high level levantadas
    - Chama Validate

The next step, after you use DataReader is to use the validation document to apply

