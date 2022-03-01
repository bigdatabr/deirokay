========
Concepts
========

Here you can find the description about key concepts that you will see 
when you are using Deirokay.


DTypes and Threaters
====================

For Deirokay, like in any other aplication, data types are important 
concept. So when you are using DataReader or Data validation, there is 
some data types that you can use to work, and here it is with there 
alias: 

* Text Type: 

    * STRING = 'string'

* Number types:

    * INT64 = 'integer'
    * FLOAT64 = 'float'

* Time types:

    * DATETIME = 'datetime'
    * DATE = 'date'
    * TIME = 'time'

* Other Types :

    * BOOLEAN = 'boolean'

Other concept to keep an eye on is Threaters. When you use the
DataReader and Data validation, Deirokay use some of class to deal with
your needs when it is about threat data, like NumericTreater, 
BooleanTreater, FloatTreater, DecimalTreater, IntegerTreater, 
DateTreater , TimeTreater, StringTreater. In the future, in case you 
need new threaters you just inherits from the bases classes and build
your own threater.


Options Document
================

The process to treated an dataframe, deirokay uses the Data Reader 
module that works with the options documents. This document holds a 
parameters like `sep`( separator), `encryption` and `columns`. This 
last one specifies your parsing options that wiil be aplied in choiced 
features. You can use the DTypes to work in your described column, 
thousand separator, decimal places and format data. At the end of this
process your features will be as you would escpetc to. 

EXEMPLO


Validation Document
===================
In the validation process, the most importante thing to konw is that 
the validation document can be write for every step in the pipeline 
process to validate the dataframe or file. Here you find the pices that 
compose the document that validates your data.

1. Name & Descripiton
---------------------

Every validation document starts with the name of the dataset that will 
work on and followed by an descripiton to help that everyone can 
understand.

EXEMPLO

2. Valitadition items
---------------------

Anther topic in the Valiadation Document is the validation items, 
that are composed by the validation statements and validation scope. 
The last one defines which columns will be analised and you can pass 
one or more groups of columns. Within the scope you can pass an alias 
to the statement. To get dive into it, you can find more at 
.._Statements. Last but not least, the validation statements are need 
to you specifies what parameters you need, like the `type`, `distinct`, 
`min`, `severity`, `at_least`, `max`, when you work throught the 
collumns of your dataset.

EXEMPLO


Validation Document Result
==========================
At the end of your data validation, deirokay can organize an output 
document(.json/.yaml) that reflects your validation document and have a 
plus of the report statement,this shows if your statements pass or not 
in relational to your analisis described in the validation items in 
detail.

EXEMPLO


Profiling
=========
An extra funciton that comes with Deirokay is you can be able to 
generate a validation document from a given template DataFram for 
builtin Deirokay statements. See the exemple below:

EXEMPLO

This function should be used only as a draft for a validation document 
or as a means to quickly launch a first version with minimum efforts.By 
default, this function recives an DataFrame (that as ideally parsed
with DataReader), an document name(string) that represents the 
validation document name and the path where wil save it( like local or 
S3). At the end, auto-generated validation document as Python `dict`.
returns an. If no path are passed, no document will be save by default.

