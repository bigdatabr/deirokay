Jinja Templating and Statements based on Past Validation Data
=============================================================

Some validation statements may present dynamic behaviour, maybe
folowing a natural uptrend or downtrend movement of your data. Suppose
you expect the number of rows of your data file to possibly fluctuate
+/- 3% around a 7-day moving average. Deirokay allows you to refer to
past validation data by means of a special function called `series`.

To use it, replace a static value for your statement parameter by a
templated argument, such as the following:

.. code-block:: yaml

    name: VENDAS
    items:
    - scope:
      - WERKS01
      - PROD_VENDA
      #  When the scope has more than one column or has special characters,
      #  you should provide an `alias` string to refer to this item.
      alias: werks_prod
      statements:
      - type: row_count
        min: > 
          '{{ 0.97 * (series("VENDAS", 7).werks_prod.row_count.rows.mean()
          |default(19, true)) | float }}'
        max: >
          '{{ 1.03 * (series("VENDAS", 7).werks_prod.row_count.rows.mean()
          |default(21, true)) | float }}'


The YAML validation document above presents some new
features. A templated Jinja argument is identified by a pair of double curly
braces (`{{ }}`) surrounding its content. Deirokay has a special
method named `series` that you can use to retrieve past data as a
`pandas.Series` object.
When declared, `series` receives a validation document name and a
number of logs to look behind. Next, you should provide a path to a
statement report value, following the sequence:
scope (possibly aliased) => statement type name => statement result
metric name. This returns a `pandas.Series` object you can take any
calculation from (`mean`, `min`, `max`, etc.).

To illustrate this path, take the templated argument from the YAML
document above as an example:

.. code-block:: python

    series("VENDAS", 7).werks_prod.row_count.rows.mean()


- `series("VENDAS", 7)`: Retrieve the 7 last validation logs for "VENDAS";
- `werks_prod`: Consider the `werks_prod`-aliased item (could be the scope value itself if it is a string for a single column name);
- `row_count`: Use statistics from `row_count` statement;
- `rows`: Use the `rows` metric reported by the selected statement (you should know the statistics reported by your statement).
- `mean()`: Since the previous object is already a pandas Series, this call gets its mean value.

When your validation results have no history, this call returns `None`.
In Jinja, you may provide a default value to replace a null variable
by using `|default(<default value>, true)`.

Finally, here is an example of validation result log. Notice that the
templates are already rendered before being saved:

.. code-block:: yaml

    name: VENDAS
    items:
    - scope:
      - WERKS01
      - PROD_VENDA
      alias: werks_prod
      statements:
      - type: row_count
        min: 1490.0
        max: 1510.0
        report:
          detail:
            rows: 1500
          result: pass
