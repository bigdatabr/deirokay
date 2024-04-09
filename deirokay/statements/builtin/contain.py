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
    * `allowed_perc_error`: The allowed percentage error when using the rule
      `only` and `all`. For example, if you define an `only` rule with the
      values `["a", "b", "c"], but value "d" was also in the df, with a 25%
      error allowed, the validation will pass, that is, at most 25% of the
      unique values on the df weren't on the `values` list.
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
    to all the `values` declared, and only these. It means you
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
      contain the values `controller` and `worker`.
      You want to be sure that there is always one and only one controller
      server in the data.

    .. code-block:: json

        {
            "scope": "role",
            "statements": [
                {
                    "type": "contain",
                    "rule": "all",
                    "values": ["controller"],
                    "parser": {"dtype": "string"},
                    "min_occurrences": 1,
                    "max_occurrences": 1
                }
            ]
        }

    You may also extend the previous example by making some adjustments
    to ensure that there is no other value than `controller` and `worker`
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
                    "values": ["controller", "worker"],
                    "parser": {"dtype": "string"},
                    "occurrences_per_value": [
                        {
                            "values": ["controller"],
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

    name = "contain"
    expected_parameters = [
        "rule",
        "values",
        "multicolumn",
        "parser",
        "parsers",
        "min_occurrences",
        "max_occurrences",
        "allowed_perc_error",
        "occurrences_per_value",
        "report_limit",
    ]
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    DEFAULT_REPORT_LIMIT = 32

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.rule = self.options["rule"]
        assert self.rule in ("all", "only", "all_and_only")
        self.multicolumn = self.options.get("multicolumn", False)
        _parsers = self.options.get("parser") or self.options["parsers"]
        if self.multicolumn:
            self.parsers = _parsers
        else:
            self.parsers = [_parsers]
        self.treaters = [
            get_treater_instance(parser, backend=self.get_backend())
            for parser in self.parsers
        ]

        self.min_occurrences = self.options.get("min_occurrences", 0)
        self.max_occurrences = self.options.get("max_occurrences", numpy.inf)
        self.occurrences_per_value = self.options.get("occurrences_per_value", [])
        self.allowed_perc_error = self.options.get("allowed_perc_error", 0)
        self.report_limit = self.options.get("report_limit", NODEFAULT)

    def _generate_analysis(self, value_counts):
        if self.multicolumn:

            def _unpack_row(row, *args):
                return (*row, *args)

        else:

            def _unpack_row(row, *args):
                return (row, *args)

        listed_values = [
            _unpack_row(row, self.min_occurrences, self.max_occurrences)
            for row in self.options["values"]
        ]
        occurences_per_value = [
            _unpack_row(
                row,
                noneor(item.get("min_occurrences"), self.min_occurrences),
                noneor(item.get("max_occurrences"), self.max_occurrences),
            )
            for item in self.occurrences_per_value
            for row in item["values"]
        ]

        occurrence_limits = pandas.DataFrame(
            occurences_per_value + listed_values,
            columns=value_counts.index.names + ["min", "max"],
        )
        options = {
            col: parser for col, parser in zip(value_counts.index.names, self.parsers)
        }
        data_treater(occurrence_limits, options, backend=Backend.PANDAS)

        occurrence_limits.drop_duplicates(
            subset=value_counts.index.names, keep="first", inplace=True
        )
        occurrence_limits["in_values"] = True
        occurrence_limits.set_index(value_counts.index.names, inplace=True)

        analysis = (
            value_counts.to_frame()
            .reset_index()
            .merge(occurrence_limits.reset_index(), how="outer")
        )
        analysis["count"].fillna(0, inplace=True)
        analysis["min"].fillna(0, inplace=True)
        analysis["max"].fillna(numpy.inf, inplace=True)
        analysis["in_values"].fillna(False, inplace=True)
        analysis["occurrences_result"] = analysis["count"].ge(
            analysis["min"]
        ) & analysis["count"].le(analysis["max"])
        return analysis

    def _generate_report(self, analysis):
        values_report = self._create_occurrences_result(analysis)
        values_report += self._create_in_values_result(analysis)
        values_report = sorted(values_report, key=lambda x: x["result"])

        if (
            self.report_limit is NODEFAULT
            and len(values_report) > Contain.DEFAULT_REPORT_LIMIT
        ):
            self.report_limit = Contain.DEFAULT_REPORT_LIMIT
            warnings.warn(
                "The 'contain' statement's report size was automatically"
                f" truncated to {Contain.DEFAULT_REPORT_LIMIT} items to"
                "  prevent unexpectedly long logs.\n"
                "If you wish to set a different"
                " size limit or even not set a limit at all (None),"
                " please declare the `report_limit` parameter explicitely.",
                Warning,
            )

        return {
            "values": (
                values_report
                if self.report_limit is NODEFAULT
                else values_report
                if self.report_limit is None
                else values_report[: self.report_limit]
            )
        }

    def _create_occurrences_result(self, analysis):
        columns = [analysis[col] for col in analysis.columns[:-4]]
        serialized = (
            treater.serialize(column) for treater, column in zip(self.treaters, columns)
        )
        rows = zip(*(s["values"] for s in serialized))
        rows = (list(row) for row in rows)
        values_report = [
            {
                "value": value_row,
                "count": analysis_row.count,
                "result": analysis_row.occurrences_result,
            }
            for value_row, analysis_row in zip(rows, analysis.itertuples())
        ]
        return values_report

    def _create_in_values_result(self, analysis):
        if self.rule == "only":
            values_in_df = analysis[(analysis["count"] > 0)]
            perc_in_values = float(values_in_df["in_values"].mean() * 100)
            return [
                {
                    "rule": self.rule,
                    "perc_in_values": perc_in_values,
                    "allowed_perc_error": self.allowed_perc_error,
                    "result": perc_in_values >= 100 - self.allowed_perc_error,
                }
            ]
        elif self.rule == "all":
            in_values = analysis[(analysis["in_values"]) & (analysis["max"] > 0)]
            if len(in_values):
                perc_in_df = float((in_values["count"] > 0).mean()) * 100
                return [
                    {
                        "rule": self.rule,
                        "perc_in_df": perc_in_df,
                        "allowed_perc_error": self.allowed_perc_error,
                        "result": perc_in_df >= 100 - self.allowed_perc_error,
                    }
                ]
            else:
                return []
        else:
            return []

    @report(Backend.PANDAS)
    def _report_pandas(self, df: "pandas.DataFrame") -> dict:
        # Concat all columns
        _cols = df.columns.tolist()

        if not self.multicolumn:
            # Columns are assumed to be of same Dtype
            df = pandas.concat([df[col] for col in _cols]).to_frame()

        value_counts = df.groupby(_cols, dropna=False)[_cols[0]].size().rename("count")
        analysis = self._generate_analysis(value_counts)
        return self._generate_report(analysis)

    @report(Backend.DASK)
    def _report_dask(self, df: "dask.dataframe.DataFrame") -> dict:
        # Concat all columns
        _cols = df.columns.tolist()

        if not self.multicolumn:
            # Columns are assumed to be of same Dtype
            df = dask.dataframe.concat([df[col] for col in _cols]).to_frame()

        value_counts = df.groupby(_cols, dropna=False)[_cols[0]].size().rename("count")
        analysis = self._generate_analysis(value_counts.compute())
        return self._generate_report(analysis)

    # docstr-coverage:inherited
    def result(self, report: dict) -> bool:
        return report["values"][0]["result"] == True

    @profile(Backend.PANDAS)
    @staticmethod
    def _profile_pandas(df: "pandas.DataFrame") -> DeirokayStatement:
        if any(dtype != df.dtypes for dtype in df.dtypes):
            raise NotImplementedError("Refusing to mix up different types of columns")

        series = pandas.concat(df[col] for col in df.columns)

        unique_series = series.drop_duplicates().dropna()
        if len(unique_series) > 20:
            raise NotImplementedError("Won't generate too long statements!")

        value_frequency = series.value_counts()
        min_occurrences = int(value_frequency.min())

        statement_template = {
            "type": "contain",
            "rule": "all",
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
        statement_template["values"].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template["min_occurrences"] = min_occurrences

        return statement_template

    @profile(Backend.DASK)
    @staticmethod
    def _profile_dask(df: "dask.dataframe.DataFrame") -> DeirokayStatement:
        if any(dtype != df.dtypes for dtype in df.dtypes):
            raise NotImplementedError("Refusing to mix up different types of columns")

        series = dask.dataframe.concat([df[col] for col in df.columns])

        unique_series = series.drop_duplicates().dropna()
        if len(unique_series) > 20:
            raise NotImplementedError("Won't generate too long statements!")

        value_frequency = series.value_counts()
        min_occurrences = int(value_frequency.min().compute())

        statement_template = {
            "type": "contain",
            "rule": "all",
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
        statement_template["values"].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template["min_occurrences"] = min_occurrences

        return statement_template
