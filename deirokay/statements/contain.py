from numpy import inf
from pandas import concat

from ..parser import get_dtype_treater, get_treater_instance
from .base_statement import BaseStatement


class Contain(BaseStatement):
    """
    Checks if a given column contains specific values. We can also
    check the number of their occurrences, specifying a minimum and
    maximum value of frequency.
    """
    name = 'contain'
    expected_parameters = [
        'rule',
        'values',
        'parser',
        'occurrences_per_value',
        'min_occurrences',
        'max_occurrences',
        'verbose'
    ]
    table_only = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rule = self.options['rule']
        self.treater = get_treater_instance(self.options['parser'])
        self.values = self.treater(self.options['values'])

        self.min_occurrences = self.options.get('min_occurrences', None)
        self.max_occurrences = self.options.get('max_occurrences', None)
        self.occurrences_per_value = self.options.get(
            'occurrences_per_value', []
        )
        self.verbose = self.options.get('verbose', True)

        self._set_default_minmax_occurrences()
        self._assert_parameters()

    def _set_default_minmax_occurrences(self):
        min_occurrences_rule_default = {
            'all': 1,
            'only': 0,
            'all_and_only': 1
        }
        max_occurrences_rule_default = {
            'all': inf,
            'only': inf,
            'all_and_only': inf
        }

        if self.min_occurrences is None:
            self.min_occurrences = min_occurrences_rule_default[self.rule]
        if self.max_occurrences is None:
            self.max_occurrences = max_occurrences_rule_default[self.rule]

    def _assert_parameters(self):
        assert self.rule in ('all', 'only', 'all_and_only')
        assert self.min_occurrences >= 0
        assert self.max_occurrences >= 0

    # docstr-coverage:inherited
    def report(self, df):
        # Concat all columns
        count_isin = (
            concat(df[col] for col in df.columns).value_counts()
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
    def result(self, report):
        self._set_min_max_boundaries(self.value_count)
        self._set_values_scope()

        if not self._check_interval(self.value_count):
            return False
        if not self._check_rule(self.value_count):
            return False
        return True

    def _set_min_max_boundaries(self, value_count):
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

    def _check_interval(self, value_count):
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

    def _check_rule(self, value_count):
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
        elif self.rule == 'all_and_only':
            is_check_all = self._check_all(value_count)
            is_check_only = self._check_only(value_count)
            return is_check_all and is_check_only

    def _check_all(self, value_count):
        """
        Checks if values in df contains all the expected values
        """
        values_in_col = set(value_count.keys())
        values = set(self.values_scope_filter)
        if values - values_in_col:
            return False
        return True

    def _check_only(self, value_count):
        """
        Checks if all values in df are inside the expected values
        """
        values_in_col = set(value_count.keys())
        values = set(self.values_scope_filter)
        if values_in_col - values:
            return False
        return True

    # docstr-coverage:inherited
    @staticmethod
    def profile(df):
        series = concat(df[col] for col in df.columns)

        value_frequency = series.value_counts()
        min_occurrences = int(value_frequency.min())

        # unique series
        series = series.drop_duplicates().dropna()
        if len(series) > 20:
            raise NotImplementedError("Won't generate too long statements!")

        statement_template = {
            'type': 'contain',
            'rule': 'all'
        }
        # Get most common type to infer treater
        try:
            statement_template.update(
                get_dtype_treater(series.map(type).mode()[0])
                .serialize(series)
            )
        except TypeError:
            raise NotImplementedError("Can't handle mixed types")
        statement_template['values'].sort(key=lambda x: (x is None, x))

        if min_occurrences != 1:
            statement_template['min_occurrences'] = min_occurrences

        return statement_template
