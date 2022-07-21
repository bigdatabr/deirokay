"""
Statement to check the presence (or absence) of values in a scope.
"""
import warnings
from typing import List

import dask.dataframe  # lazy module
import numpy  # lazy module
import pandas  # lazy module

from deirokay._typing import DeirokayStatement
from deirokay._utils import noneor
from deirokay.enums import Backend
from deirokay.parser import get_dtype_treater, get_treater_instance
from deirokay.parser.loader import data_treater

from ..multibackend import profile, report
from .base_statement import BaseStatement

NODEFAULT = object()


class Contain(BaseStatement):
    """
    Checks if the given scope contains specific values. You may also
    check the number of their occurrences by specifying a minimum and
    maximum value of frequency.

    The available parameters for this statement are:

    * `rule` (required): One of `all`, `only` or `all_and_only`.
    * `values` (required): A list of values to which the rule applies.
    * `multicolumn`: A boolean indicating whether the statement should
      consider each of the `values` as a tuple of multiple columns or
      a single value. When set to False and evaluated over a scope
      containing more than one column, the rule will be applied over
      all the values from the original columns as in a single column.
      Default: False.
    * `parser`: The parser (or a list) to be used to parse the `values`.
      Correspond to the `parser` parameter of the `treater` function
      (see `deirokay.data_reader` method).
      Either `parser` or `parsers` must be declared.
    * `parsers`: An alias for `parser`, recommended when `multicolumn`
      is set to True.
      Either `parser` or `parsers` must be declared.
    * `min_occurrences`: a global minimum number of occurrences for
      each of the `values`. Default: 1 for `all` and `all_and_only`
      rules, 0 for `only`.
    * `max_occurrences`: a global maximum number of occurrences for
      each of the `values`. Default: `inf` (unbounded).
    * `occurrences_per_value`: a list of dictionaries overriding the
      global boundaries. Each dictionary may have the following keys:

        * `values` (required): a list of values to which the
          occurrence bounds below must apply to.
        * `min_occurrences`: a minimum number of occurrences for these
          values. Default: global `min_occurrences` parameter.
        * `max_occurrences`: a maximum number of occurrences for these
          values. Default: global `max_occurrences` parameter.

      Global parameters apply to all values not present in any of the
      dictionaries in `occurrences_per_value` (but yet present on the
      main `values` list).

    * `report_limit`: if set to a positive integer, limit the number of
      items generated in the statement report.
      Default: 32.

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

    Null values are considered valid for the purpose of the statement
    evaluation and must be explicitely passed in `values` if you wish
    to allow them (or not).

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

    * You want the pair 'San Diego' and '2022' to appear at most twice
      in your dataset (for any reason).

    .. code-block:: yaml

        scope: [city, year]
        statements:
        - type: contain
          rule: all
          min_occurrences: 0
          max_occurrences: 2
          values:
          - ['San Diego', 2022]
          parsers:
          - {dtype: string}
          - {dtype: integer}

    """
    name = 'contain'
    expected_parameters = [
        'rule',
        'values',
        'multicolumn',
        'parser',
        'parsers',
        'min_occurrences',
        'max_occurrences',
        'occurrences_per_value',
        'report_limit',
    ]
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    DEFAULT_MIN_OCCURRENCES = {
        'all': (1, 0),
        'only': (0, 0),
        'all_and_only': (1, 0)
    }
    DEFAULT_MAX_OCCURRENCES = {
        'all': (numpy.inf, numpy.inf),
        'only': (numpy.inf, 0),
        'all_and_only': (numpy.inf, 0)
    }
    DEFAULT_REPORT_LIMIT = 32

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.rule = self.options['rule']
        assert self.rule in ('all', 'only', 'all_and_only')
        self.multicolumn = self.options.get('multicolumn', False)
        _parsers = self.options.get('parser') or self.options['parsers']
        if self.multicolumn:
            self.parsers = _parsers
        else:
            self.parsers = [_parsers]
        self.treaters = [
            get_treater_instance(parser, backend=self.get_backend())
            for parser in self.parsers
        ]

        self.min_occurrences = self.options.get('min_occurrences', None)
        self.max_occurrences = self.options.get('max_occurrences', None)
        self.occurrences_per_value = self.options.get(
            'occurrences_per_value', []
        )
        self.report_limit = self.options.get('report_limit', NODEFAULT)

        self._set_default_minmax_occurrences()

    def _set_default_minmax_occurrences(self) -> None:
        final_min = noneor(self.min_occurrences,
                           Contain.DEFAULT_MIN_OCCURRENCES[self.rule][0])
        final_max = noneor(self.max_occurrences,
                           Contain.DEFAULT_MAX_OCCURRENCES[self.rule][0])
        if (
            self.max_occurrences is not None and
            self.max_occurrences < final_min
        ):
            final_min = self.max_occurrences
        if (
            self.min_occurrences is not None and
            self.min_occurrences > final_max
        ):
            final_max = self.min_occurrences
        assert final_min >= 0
        assert final_max >= 0
        self.min_occurrences = final_min
        self.max_occurrences = final_max

    def _generate_analysis(self, value_counts):
        if self.multicolumn:
            def _unpack_row(row, *args):
                return (*row, *args)
        else:
            def _unpack_row(row, *args):
                return (row, *args)

        listed_values = [
            _unpack_row(
                row,
                self.min_occurrences,
                self.max_occurrences
            )
            for row in self.options['values']
        ]
        occurences_per_value = [
            _unpack_row(
                row,
                noneor(item.get('min_occurrences'), self.min_occurrences),
                noneor(item.get('max_occurrences'), self.max_occurrences),
            )
            for item in self.occurrences_per_value
            for row in item['values']
        ]

        occurrence_limits = pandas.DataFrame(
            occurences_per_value + listed_values,
            columns=value_counts.index.names + ['min', 'max']
        )
        options = {
            col: parser
            for col, parser in zip(value_counts.index.names, self.parsers)
        }
        data_treater(occurrence_limits, options, backend=Backend.PANDAS)

        occurrence_limits.drop_duplicates(
            subset=value_counts.index.names,
            keep='first',
            inplace=True
        )
        occurrence_limits.set_index(value_counts.index.names, inplace=True)

        analysis = value_counts.to_frame().reset_index().merge(
            occurrence_limits.reset_index(), how='outer'
        )
        analysis['count'].fillna(0, inplace=True)
        analysis['min'].fillna(Contain.DEFAULT_MIN_OCCURRENCES[self.rule][1],
                               inplace=True)
        analysis['max'].fillna(Contain.DEFAULT_MAX_OCCURRENCES[self.rule][1],
                               inplace=True)
        analysis['result'] = (
            analysis['count'].ge(analysis['min'])
            &
            analysis['count'].le(analysis['max'])
        )
        return analysis

    def _generate_report(self, analysis):
        columns = [analysis[col] for col in analysis.columns[:-4]]
        serialized = (
            treater.serialize(column)
            for treater, column in zip(self.treaters, columns)
        )
        rows = zip(*(s['values'] for s in serialized))
        values_report = sorted([
            {
                'value': value_row,
                'count': analysis_row.count,
                'result': analysis_row.result,
            }
            for value_row, analysis_row in zip(rows, analysis.itertuples())
        ], key=lambda x: x['result'])

        if (
            self.report_limit is NODEFAULT and
            len(values_report) > Contain.DEFAULT_REPORT_LIMIT
        ):
            self.report_limit = Contain.DEFAULT_REPORT_LIMIT
            warnings.warn(
                "The 'contain' statement's report size was automatically"
                f' truncated to {Contain.DEFAULT_REPORT_LIMIT} items to'
                '  prevent unexpectedly long logs.\n'
                'If you wish to set a different'
                ' size limit or even not set a limit at all (None),'
                ' please declare the `report_limit` parameter explicitely.',
                Warning
            )

        return {
            'values': (
                values_report if self.report_limit is NODEFAULT else
                values_report if self.report_limit is None else
                values_report[:self.report_limit]
            )
        }

    @report(Backend.PANDAS)
    def _report_pandas(self, df: 'pandas.DataFrame') -> dict:
        # Concat all columns
        _cols = df.columns.tolist()

        if not self.multicolumn:
            # Columns are assumed to be of same Dtype
            df = pandas.concat([df[col] for col in _cols]).to_frame()

        value_counts = (
            df.groupby(_cols, dropna=False)[_cols[0]].size()
            .rename('count')
        )
        analysis = self._generate_analysis(value_counts)
        return self._generate_report(analysis)

    @report(Backend.DASK)
    def _report_dask(self, df: 'dask.dataframe.DataFrame') -> dict:
        # Concat all columns
        _cols = df.columns.tolist()

        if not self.multicolumn:
            # Columns are assumed to be of same Dtype
            df = dask.dataframe.concat([df[col] for col in _cols]).to_frame()

        value_counts = (
            df.groupby(_cols, dropna=False)[_cols[0]].size()
            .rename('count')
        )
        analysis = self._generate_analysis(value_counts.compute())
        return self._generate_report(analysis)

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        return all(
            item['result'] for item in report['values']
        )

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
        # Sort allowing `None` values, which will appear last
        statement_template['values'].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template['min_occurrences'] = min_occurrences

        return statement_template

    @profile(Backend.DASK)
    @staticmethod
    def _profile_dask(df: 'dask.dataframe.DataFrame') -> DeirokayStatement:
        if any(dtype != df.dtypes for dtype in df.dtypes):
            raise NotImplementedError(
                "Refusing to mix up different types of columns"
            )

        series = dask.dataframe.concat([df[col] for col in df.columns])

        unique_series = series.drop_duplicates().dropna()
        if len(unique_series) > 20:
            raise NotImplementedError("Won't generate too long statements!")

        value_frequency = series.value_counts()
        min_occurrences = int(value_frequency.min().compute())

        statement_template = {
            'type': 'contain',
            'rule': 'all'
        }  # type: DeirokayStatement
        # Get most common type to infer treater
        try:
            statement_template.update(
                get_dtype_treater(unique_series.map(type).mode().compute()[0])
                .attach_backend(Backend.DASK)
                .serialize(unique_series)  # type: ignore
            )
        except TypeError:
            raise NotImplementedError("Can't handle mixed types")
        # Sort allowing `None` values, which will appear last
        statement_template['values'].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template['min_occurrences'] = min_occurrences

        return statement_template
