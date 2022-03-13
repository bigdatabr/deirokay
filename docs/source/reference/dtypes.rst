DTypes
======

For Deirokay, as in any other application, data types are an important 
concept. So, when you are parsing your data with Deirokay, there are 
a few datatypes that you might want to make use of.

All available datatypes are defined in the :ref:`deirokay.enums.DTypes` enumeration class.
Each Deirokay Dtype is associated to a Treater class from :ref:`deirokay.parser.treaters`, which is responsible for parsing and converting the data to the specified datatype.

Below you will find a list of supported datatypes and available parameters:

+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DTypes     | String-like alias | Supported Arguments  | Default             | Argument Description                   |
+============+===================+======================+=====================+========================================+
| All DTypes |                   | nullable             | True                | Values can be null                     |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| All DTypes |                   | unique               | False               | Values should be unique                |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| All DTypes |                   | rename               | None                | Rename column                          |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| INTEGER    | 'integer'         | thousand_sep         | None                | Thousand separator (e.g., "1,988")     |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| FLOAT      | 'float'           | thousand_sep         | None                | Thousand separator (e.g., "1,988")     |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| FLOAT      | 'float'           | decimal_sep          | '.'                 | Decimal separator (e.g., "3.14")       |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DECIMAL    | 'decimal'         | decimal_sep          | '.'                 | Decimal separator (e.g., "3.14")       |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DECIMAL    | 'decimal'         | thousand_sep         | None                | Thousand separator (e.g., "1,988")     |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DECIMAL    | 'decimal'         | decimal_places       | None                | Decimal places (e.g., 2 for "1.25")    |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| BOOLEAN    | 'boolean'         | truthies             | ['true','True']     | Values taken as True                   |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| BOOLEAN    | 'boolean'         | falsies              | ['false', 'False']  | Values taken as False                  |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| BOOLEAN    | 'boolean'         | ignore_case          | False               | Ignore case when evaluating True/False |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| BOOLEAN    | 'boolean'         | default_value        | None                | Value to use if not truthy nor falsy   |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DATETIME   | 'datetime'        | format               | '%Y-%m-%d %H:%M:%S' | Date Time format                       |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| TIME       | 'time'            | format               | '%H:%M:%S'          | Time format                            |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| DATE       | 'date'            | format               | '%Y-%m-%d'          | Date format                            |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
| STRING     | 'string'          | treat_null_as        | None                | Value to replace when null (e.g., "")  |
+------------+-------------------+----------------------+---------------------+----------------------------------------+
