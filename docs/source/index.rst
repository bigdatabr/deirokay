===============
Deirokay - Home
===============

Deirokay (*dejɾo'kaj*) is a tool for data parsing and data validation.
Deirokay separates document parsing from validation 
logic, so that you can create your own statements about your data
being sure that your source data has been properly parsed.

Why should I use Deirokay?
===========================

Here are some of the benefits:

* **Simple to use**: instead of writing your data validation in pure Python, you can create your rules in a simple and intuitive language with just a few lines of configuration. This leads to a more human-readable format to express your validations, making it possible for people with different levels of expertize to engage and contribute with it.
* **Centralized and versionable**: all validations for a same data source are present in a single document, so that you can easily keep track of the rules that have been added, removed or modified with the help of your preferred versioning system;
* **Shareable configuration**: by having a simple language, you can easily replicate your statements in different validation documents or in different projects, so that you can share the same validation rules with your colleagues;
* **Logging**: meaningful logs containing relevant statistics that you may use to understand your data, find sources of errors and even reuse in your next validations;
* **Customizable**: you may create and reuse your own statement classes to express rules tailored to your needs, instead of being limited to the standard library.
* **Multiple sources**: you can validate data from multiple sources, including files, databases or existing Pandas DataFrames.

Main Features
=============
Deirokay comes with several features to help you in your daily 
data quality problems. The main tools that facilitate your job
are *Data Reader*, *Data Validation* and *Data Profiling*.

The :ref:`deirokay.data_reader<deirokay.parser.parser>` method is responsible for parsing and transforming your source data into a trustable DataFrame before it gets validated. You may get a DataFrame from a file, a database or an existing Pandas DataFrame that has not been treated yet, all you need to do is to provide a Deirokay Options Document specifying the parsing options.
Deirokay Data Reader can also be imported and used in your project to improve your analysis and data manipulation when using Pandas.

Data Validation is performed by the :ref:`deirokay.validate<deirokay.validator.validate>` method and works with the help of Deirokay Validation Documents. The latter must be created for each data file and they gather all the validation (using native or custom statements) to be executed against the data. 
Every validation results in a *Validation Result Document*, which you may choose to log for reference or later use.

Data Profiling is provided by the :ref:`deirokay.profile<deirokay.profiler.profile>` method and is here to help you with automatically generating Validation Documents based on template data. 
The user is encouraged to modify and supplement the generated document to better meet their requirements.


Why use Deirokay instead of other data quality tools?
======================================================

Although Deirokay is still in its early stages, it is already
a powerful tool that can be used to validate data from any
source. It is also a tool that can be used to generate
validation documents based on a template.

In comparison to the most popular open-source alternative, `Great Expectations <https://docs.greatexpectations.io/docs/>`_, Deirokay was made to allow a more human-readable and more flexible configuration of data validation with its *Validation Document*. Besides that, Great Expectations requires you to open a Jupyter notebook in order to generate a valid Expectations file, and although such a notebook comes already pre-populated, their API is not as easy to understand as Deirokay's. The resulting Expectations file is too verbose and not suitable for people with lower levels of expertise.

Deirokay also provides a better logging system that allows you to reuse past validation results in your next validation process, making it possible to test your data using dynamic parameters such as moving averages.

Deirokay also integrates easily with `Apache Airflow <https://airflow.apache.org/docs/>`_ with its built-in :ref:`DeirokayOperator<deirokay.airflow>`.

On the other hand, Great Expectations is a well consolidated tool with great community support and already used in a lot of projects. It already has a vast set of builtin expectations (analogous to Deirokay Statements), a cli tool to manage your configurations and also generates *Data Docs* for visualization of your validations (which unfortunately still lacks of offline support, requiring you to be online to download external `.js` and `.css` and be able to view the docs).


Documentation TODOs
===================

.. todolist::


.. include:: contents.rst
