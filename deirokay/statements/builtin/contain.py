"""
Statement to check the presence (or absence) of values in a scope.
"""
from typing import List

import numpy  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayStatement
from deirokay.enums import Backend
from deirokay.parser import get_dtype_treater, get_treater_instance

from ..multibackend import profile, report
from .base_statement import BaseStatement


class Contain(BaseStatement):
    """
    Checks if the given scope contains specific values. We can also
    check the number of their occurrences by specifying a minimum and
    maximum value of frequency.

    The available parameters for this statement are:

    * `rule` (required): One of `all`, `only` or `all_and_only`.
    * `values` (required): A list of values to which the rule applies.
    * `parser` (required): The parser to be used to parse the `values`.
      Correspond to the `parser` parameter of the `treater` function
      (see `deirokay.data_reader` method).
    * `min_occurrences`: a global minimum number of occurrences for
      each of the `values`. Default: 1 for `all` and `all_and_only`
      rules, 0 for `only`.
    * `max_occurrences`: a global maximum number of occurrences for
      each of the `values`. Default: `inf` (unbounded).
    * `occurrences_per_value`: a list of dictionaries overriding the
      global boundaries. Each dictionary may have the following keys:

        * `values` (required): a value (or a list) to which the
          occurrence bounds below must apply to.
        * `min_occurrences`: a minimum number of occurrences for these
          values. Default: global `min_occurrences` parameter.
        * `max_occurrences`: a maximum number of occurrences for these
          values. Default: global `max_occurrences` parameter.

      Global parameters apply to all values not present in any of the
      dictionaries in `occurrences_per_value` (but yet present on the
      main `values` list).

    * `verbose`: if `True`, the report will include the percentage of
      occurrences for each value. Default: `True`.

    The `all` rule checks if all the `values` declared are present in
    the scope (possibly tolerating other values not declared in
    `values`).
    Use it when you want to be sure that your data contains at least
    all the values you declare, also setting `min_occurrences` and
    `max_occurrences` when necessary.
    You may also check for "zero" occurrences of a set of values by
    setting `max_occurrences` to 0.

    The `only` rule ensures that the `values` are the only possible
    values in the scope (possibly not containing them at all).
    Use it when you want to enumerate the admitted values for the
    scope, as in an enumeration.

    The `all_and_only` rule checks both if all the `values` declared
    are present in the scope and if only they are present (not
    tolerating values not declared).
    Use it when you know all the possible values for the scope and you
    are sure that they will be always present.

    The `min_occurrences` and `max_occurrences` parameters are applied
    applied to all the `values` declared, and only these. It means you
    cannot (yet) specify boundaries for values you did't declare.

    You may also notice that, by tweaking the expected number of
    occurrences, you may end up having the very same behaviour
    regardless the `rule` you choose.
    In this case, you should go for the rule that semantically matches
    best your intents, so that your final validation document looks
    more readable and easy to understand.

    Examples
    --------
    * You have a table of users containg a column `handedness`
      only admitting the values:
      `right-handed`, `left-handed` and `ambidextrous`.
      You know that some of these values may not appear in the data,
      but you don't want other values to be present.

    .. code-block:: json

        {
            "scope": "handedness",
            "statements": [
                {
                    "type": "contain",
                    "rule": "only",
                    "values": ["right-handed", "left-handed", "ambidextrous"],
                    "parser": {"dtype": "string"}
                }
            ]
        }

    * You have a table of servers containg a column `role` which may
      contain the values `master` and `slave`.
      You want to be sure that there is always one and only one master
      server in the data.

    .. code-block:: json

        {
            "scope": "role",
            "statements": [
                {
                    "type": "contain",
                    "rule": "all",
                    "values": ["master"],
                    "parser": {"dtype": "string"},
                    "min_occurrences": 1,
                    "max_occurrences": 1
                }
            ]
        }

    You may also extend the previous example by making some adjustments
    to ensure that there is no other value than `master` and `role`
    in the data. Make notice that although the `rule` below is changed
    to `only`, the statement above is still contemplated by the
    `occurrences_per_value` parameter in the following validation item:

    .. code-block:: json

        {
            "scope": "role",
            "statements": [
                {
                    "type": "contain",
                    "rule": "only",
                    "values": ["master", "slave"],
                    "parser": {"dtype": "string"},
                    "occurrences_per_value": [
                        {
                            "values": ["master"],
                            "min_occurrences": 1,
                            "max_occurrences": 1
                        }
                    ]
                }
            ]
        }

    * You have a table of transactions containing details about
      transactions in all the branches of a company. You expect that
      there should always be at least one transaction per branch.

    .. code-block:: json

        {
            "scope": ["branch_name"],
            "statements": [
                {
                    "type": "contain",
                    "rule": "all_and_only",
                    "values": [
                        "Albany", "Utica", "Scranton", "Akron",
                        "Nashua", "Buffalo", "Rochester"
                    ],
                    "parser": {"dtype": "string"}
                }
            ]
        }

    * You have a table for the logs of user accesses to a website which
      contains an `IP` column. You want to be sure that blacklisted
      IPs are not present in the data. The following validation item in
      YAML format checks for the absense of blacklisted IPs:

    .. code-block:: yaml

        scope: IP
        statements:
        - type: contain
          rule: all
          max_occurrences: 0
          values: # blacklisted IPs
          - 3.48.48.135
          - 3.48.48.136
          parser: {dtype: string}

    """
    name = 'contain'
    expected_parameters = [
        'rule',
        'values',
        'parser',
        'min_occurrences',
        'max_occurrences',
        'occurrences_per_value',
        'verbose'
    ]
    supported_backends: List[Backend] = [Backend.PANDAS]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.rule = self.options['rule']
        self.treater = get_treater_instance(self.options['parser'],
                                            backend=self.get_backend())
        self.values = self.treater(self.options['values'])

        self.min_occurrences = self.options.get('min_occurrences', None)
        self.max_occurrences = self.options.get('max_occurrences', None)
        self.occurrences_per_value = self.options.get(
            'occurrences_per_value', []
        )
        self.verbose = self.options.get('verbose', True)

        self._set_default_minmax_occurrences()
        self._assert_parameters()

    def _set_default_minmax_occurrences(self) -> None:
        min_occurrences_rule_default = {
            'all': 1,
            'only': 0,
            'all_and_only': 1
        }
        max_occurrences_rule_default = {
            'all': numpy.inf,
            'only': numpy.inf,
            'all_and_only': numpy.inf
        }

        if self.min_occurrences is None:
            self.min_occurrences = min_occurrences_rule_default[self.rule]
        if self.max_occurrences is None:
            self.max_occurrences = max_occurrences_rule_default[self.rule]

    def _assert_parameters(self) -> None:
        assert self.rule in ('all', 'only', 'all_and_only')
        assert self.min_occurrences >= 0
        assert self.max_occurrences >= 0

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        # Concat all columns
        count_isin = (
            pandas.concat(df[col] for col in df.columns).value_counts()
        )
        self.value_count = count_isin.to_dict()

        # Generate report
        values = self.treater.serialize(self.value_count.keys())['values']
        freqs = [int(freq) for freq in self.value_count.values()]

        if self.verbose:
            # Include percentage in reports
            total = int(count_isin.sum())
            rel_freqs = (freq*100/total for freq in freqs)
            values_report = [
                {'value': value, 'count': freq, 'perc': pfreq}
                for value, freq, pfreq in zip(values, freqs, rel_freqs)
            ]
        else:
            values_report = [
                {'value': value, 'count': freq}
                for value, freq in zip(values, freqs)
            ]
        return {
            'values': values_report
        }

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        self._set_min_max_boundaries()
        self._set_values_scope()

        if not self._check_interval(self.value_count):
            return False
        if not self._check_rule(self.value_count):
            return False
        return True

    def _set_min_max_boundaries(self) -> None:
        # Global boundaries
        min_max_boundaries = {}
        for value in self.values:
            min_max_boundaries.update({
                value: {
                    'min_occurrences': self.min_occurrences,
                    'max_occurrences': self.max_occurrences
                }
            })

        # Dedicated boundaries
        if self.occurrences_per_value:
            for occurrence in self.occurrences_per_value:
                values = occurrence['values']
                values = [values] if not isinstance(values, list) else values
                values = self.treater(values)

                for value in values:
                    min_max_boundaries[value][
                        'min_occurrences'
                    ] = occurrence.get(
                        'min_occurrences', self.min_occurrences
                    )

                    min_max_boundaries[value][
                        'max_occurrences'
                    ] = occurrence.get(
                        'max_occurrences', self.max_occurrences
                    )

        self.min_max_boundaries = min_max_boundaries

    def _set_values_scope(self):
        """
        Set the scope of values to be analyzed according to the given
        `self.rule`. Excludes the cases of values where its
        corresponding `max_occurrences` is zero, since these cases
        won't matter for the `rule` analysis, as they must be absent
        in the column.
        """
        values_col = [
            value for value in self.min_max_boundaries
            if self.min_max_boundaries[value]['max_occurrences'] != 0
        ]
        self.values_scope_filter = values_col

    def _check_interval(self, value_count: dict) -> bool:
        """
        Check if each value is inside an interval of min and max
        number of occurrencies. These values are set globally in
        `self.min_occurrencies` and `self.max_occurrencies`, but the
        user can specify dedicated intervals for a given value in
        `self.occurrences_per_value`
        """
        for value in self.values:
            min_value = self.min_max_boundaries[value][
                'min_occurrences'
            ]
            max_value = self.min_max_boundaries[value][
                'max_occurrences'
            ]
            if value in value_count:
                if not (
                    min_value <= value_count[value] <= max_value
                ):
                    return False
            else:
                if self.rule != 'only' and max_value != 0 and min_value != 0:
                    return False
        return True

    def _check_rule(self, value_count: dict) -> bool:
        """
        Checks if given columns attend the given requirements
        of presence or absence of values, according to a criteria
        specified in `self.rule`

        Parameters
        ----------
        value_count: dict
            Got from `report` method, it contains the count of
            occurrences for each column for each value

        Notes
        -----
        `self.rule` parameter defines the criteria to use for checking
        the presence or absence of values in a column. Its values
        should be:

        * all: all the values in `self.values` are present in the
        column, but there can be other values also
        * only: only the values in `self.values` (but not necessarilly
        all of them) are present in the given column
        * all_and_only: the column must contain exactly the values in
          `self.values` - neither more than less. As the name says, it
          is an `and` boolean operation between `all` and `only` modes
        """
        if self.rule == 'all':
            return self._check_all(value_count)
        elif self.rule == 'only':
            return self._check_only(value_count)
        else:
            return (self._check_all(value_count) and
                    self._check_only(value_count))

    def _check_all(self, value_count: dict) -> bool:
        """
        Checks if values in df contains all the expected values
        """
        values_in_col = set(value_count.keys())
        values = set(self.values_scope_filter)
        if values - values_in_col:
            return False
        return True

    def _check_only(self, value_count: dict) -> bool:
        """
        Checks if all values in df are inside the expected values
        """
        values_in_col = set(value_count.keys())
        values = set(self.values_scope_filter)
        if values_in_col - values:
            return False
        return True

    @profile(Backend.PANDAS)
    @staticmethod
    def _profile_pandas(df: 'pandas.DataFrame') -> DeirokayStatement:
        if any(dtype != df.dtypes for dtype in df.dtypes):
            raise NotImplementedError(
                "Refusing to mix up different types of columns"
            )

        series = pandas.concat(df[col] for col in df.columns)

        unique_series = series.drop_duplicates().dropna()
        if len(unique_series) > 20:
            raise NotImplementedError("Won't generate too long statements!")

        value_frequency = series.value_counts()
        min_occurrences = int(value_frequency.min())

        statement_template = {
            'type': 'contain',
            'rule': 'all'
        }  # type: DeirokayStatement
        # Get most common type to infer treater
        try:
            statement_template.update(
                get_dtype_treater(unique_series.map(type).mode()[0])
                .attach_backend(Backend.PANDAS)
                .serialize(unique_series)  # type: ignore
            )
        except TypeError:
            raise NotImplementedError("Can't handle mixed types")
        statement_template['values'].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template['min_occurrences'] = min_occurrences

        return statement_template
