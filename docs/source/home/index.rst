=======
Welcome
=======

Welcome to Deirokay!

Deirokay (dejɾo'kaj) is a tool for data profiling and data validation.
Deirokay it`s capable of separates document parsing from validation 
logic,so that you can create your own statements about your data
without worrying whether or not your file has been properly
parsed.

Why should i use Deirokay ?
===========================
Insted of use some validation in pure python, with Deirokay you can use
functions and a validation document. With that all validations are 
performed in a single file and leads to a very readable document by the
people connected in your project. Deirokay's validations documents can 
get you executional logs that have relevant estatistics about your 
validation and when you have created some validation document of your 
own, you can use it in another data-frame or file, so this enables you 
to track your validations with structured versioning. In addition, 
Deirokay can do parsing from files (CSV, parquet, excel, or any other 
pandas-compatible format), RDBMS, and data-frames.


Main Features
=============
With Deirokay, comes some main features to help you with your daily 
data quality problems. The main tools that facilitate your job
is DataReader, Data Validation and Data Profilling.

DataReader is responsible to read and correctly parsing files before it
gets validate. To do so, Deirokay can create a dataFrame from a file, 
or make existing dataFrame better, by appling treatments to correctly 
parse it and pre-validate its content. To start understand, DataReader 
looks for the extesion file and deal with in the proper way to 
transform it in a dataframe, or with you use a dataframe, DataReader 
threats every column in it with yours specified data types.

Data Validation works on validation document, this documents must be 
created for each data file and it gathers all the validation (native or
customized) to be performed against the data. The validation process 
works with statements and when it's done a document with results is 
generated with whenever a statement whose severity level is greater or 
equal to `exception_level` fails or pass.

Data Profilling is here to help you with automaticaly generating 
Deiroaky statements based on an existing file. To start generating an 
validation document, by default, statement objects are generated for 
the entire template DataFrame (the entire set of columns), and then for 
each of its columns individually. The user is encouraged to correct and 
supplement the generated document to better meet their expectations.


Why use Deirokay insted of other data quality tools ?
=====================================================

What is the difference between Deirokay and other data quality tools?
Some of them have limitations like doesn`t work at virtual private 
clouds or even doesn`t have an easy to use feeling. Other tools can`t 
have an easy integrations to some of main orchestrator tools, like 
Airflow, and haven`t an easy integration with S3. If you`re looking for 
a pythonic way to do your data quality jobs, Deirokay is the way, 
because it was designed to every one on AWS cloud, and not like ohter 
tools that was just build in spark or even though just need a spark 
clusters to work. Besides, Deirokay has the property (Series) of using 
moving average logs for future validation. Finally, constrution of 
statement is easer, just create a .py to works as a function.



..include:: contents.rst



