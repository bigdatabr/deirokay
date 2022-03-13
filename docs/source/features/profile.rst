Data Profiling: Auto-generated Validation Document
==================================================

You may generate a basic validation document by consuming a sample file.
It is recommended that you review the generated document and
supplement it with additional statements. The document can be saved in
either JSON or YAML format, depending on the extension of the file path
that you passed to optional `save_to` argument. You may also retrieve
the `dict` document from the method's return.

.. code-block:: python

    from deirokay import data_reader, profile


    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )

    validation_document = profile(df,
                                document_name='my-document-name',
                                save_to='my-validation-document.json')


The `profile` method can be imported from :ref:`deirokay.profile<deirokay.profiler.profile>`.
