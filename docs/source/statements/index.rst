==========
Statements
==========

A Statement is one of the main entities in Deirokay. In short, a Statement is a special rule that is executed against your data in form of test to determine whether it passes or fails.

In the context of validation, it is represented by an object of key-value pairs (also called *associative array* or *dictionary*). See the example of Validation Document below, in JSON format:

.. code-block:: json

    {
        "name": "VENDAS",
        "description": "A sample file",
        "items": [
            {
                "scope": [
                    "WERKS01",
                    "PROD_VENDA"
                ],
                "statements": [
                    {
                        "type": "unique",
                        "at_least_%": 40.0
                    },
                    {
                        "type": "not_null",
                        "at_least_%": 95.0
                    },
                    {
                        "type": "row_count",
                        "min": 18,
                        "max": 22
                    }
                ]
            }
        ]
    }

The same structure can be represented in a YAML, which is preferred for readability reasons:

.. code-block:: yaml

    name: VENDAS
    description: A sample file
    items:
        - scope:
            - WERKS01
            - PROD_VENDA
          statements:
            - type: unique
              at_least_%: 40.0
            - type: not_null
              at_least_%: 95.0
            - type: row_count
              min: 18
              max: 22

In the example above, we have three statements applied in the same scope of columns. It means that each of these statements will generate tests only over these two columns: *WERKS01* and *PROD_VENDA*.

Make notice every statement declaration identifies its *type*, which is the rule to be tested. In function of its type, a statement may have several additional parameters, and they can be optional or not.

There are special parameter names, which are actually directives to pass attributes regarding the statement itself, and not the statement type. They are:

- **type**: It defines the statement type itself, so it cannot be used as a parameter name. Should be set to *custom* in case of custom statements, and the statement class location should be declared in **location**.

- **severity**: Set a severity level for a statement in case of failure. It must be an integer value, conventionally from 1 (minimal) to 5 (critical). In one use case, you could create several similar statements setting different threshold levels for a statement parameter, each one with a different severity level.

- **location**: When declaring a custom statement, this key specifies where the statement class is stored, and what is the class name.


How Statements are processed
============================

For a statement to be processed, its type, as declared in the validation document, must match a statement class name (the *name* attribute of a statement class).

.. _deirokay/statements.py: http://gitlab.bigdata/data-engineers/deirokay/-/blob/master/deirokay/statements.py

The native statement classes are all declared in `deirokay/statements.py`_. See an example:

.. code-block:: python

    class NotNull(BaseStatement):
        """Check if the rows of a scoped DataFrame are not null."""

        name = 'not_null'
        expected_parameters = ['at_least_%', 'at_most_%', 'multicolumn_logic']

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.at_least_perc = self.options.get('at_least_%', 100.0)
            self.at_most_perc = self.options.get('at_most_%', 100.0)
            self.multicolumn_logic = self.options.get('multicolumn_logic', 'any')

            assert self.multicolumn_logic in ('any', 'all')

        def report(self, df):
            if self.multicolumn_logic == 'all':
                not_nulls = ~df.isnull().any(axis=1)
            else:
                not_nulls = ~df.isnull().all(axis=1)

            report = {
                'null_rows': int((~not_nulls).sum()),
                'null_rows_%': float(100.0*(~not_nulls).sum()/len(not_nulls)),
                'not_null_rows': int(not_nulls.sum()),
                'not_null_rows_%': float(100.0*not_nulls.sum()/len(not_nulls)),
            }
            return report

        def result(self, report):
            if not report.get('not_null_rows_%') >= self.at_least_perc:
                return False
            if not report.get('not_null_rows_%') <= self.at_most_perc:
                return False
            return True

        @staticmethod
        def profile(df):
            not_nulls = ~df.isnull().all(axis=1)

            statement = {
                'type': 'not_null',
                'multicolumn_logic': 'any',
                'at_least_%': float(100.0*not_nulls.sum()/len(not_nulls)),
                'at_most_%': float(100.0*not_nulls.sum()/len(not_nulls))
            }
            return statement

When processing statements, Deirokay will list all class names in this module and look for a class whose name matches the *type* key from the validation document. It means it is enough to create a class in this module to make it available to use.

The *expected_parameters* is a mandatory argument to identify all valid parameters for the current class. Any parameter that is neither special nor listed as expected will raise an exception.

Ideally, the statement class could validate the parameters it accepts in the *__init__* method.

The *report* method is intended to report statistics that may be useful for the current statement. Thinking of the validation process also as a form of logging (when the validation result is saved), the metrics reported by the statement could be useful in a numerous use cases. Ideally, the *report* method should also summarize all calculations that will be logically evaluated by the next method.

The *result* method has only one purpose: return either True (for a successful test) or False (for a failed test). A failure can be a consequence of several reasons, since a statement is able to evaluate a series of parameters passed by the user and a set of metrics reported by the *report* method.

The *profile* is a static method used to generate a default statement object for the current class. It is not called during the validation process, but when profiling the data. When the user calls the *deirokay.profile* function, all native statement classes having a *profile* method are iterated to generate a default statement. By default, statement objects are generated for the entire template DataFrame (the entire set of columns), and then for each of its columns individually.
