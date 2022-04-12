from pandas import DataFrame, Series

from .parser.treaters import get_treater_instance


class DeirokaySeries(Series):
    """Series object with additional options for Deirokay treaters."""

    _metadata = ['options']

    def __init__(self, data, *args, options=None, **kwargs):
        super().__init__(data, *args, **kwargs)
        self.options = options

    @property
    def _constructor(self):
        return DeirokaySeries

    @property
    def _constructor_expanddim(self):
        return DeirokayDataFrame

    def treat(self, options):
        """Treat series with the given options."""
        self.options = options
        treater = get_treater_instance(options)
        return treater.treat(self)


class DeirokayDataFrame(DataFrame):
    """DataFrame object with additional options for Deirokay treaters.
    """

    _metadata = ['options_doc']

    def __init__(self, data, *args, options_doc=None, **kwargs):
        super().__init__(data, *args, **kwargs)
        self.options_doc = options_doc

    @property
    def _constructor(self):
        return DeirokayDataFrame

    @property
    def _constructor_sliced(self):
        return DeirokaySeries

    def treat(self):
        """Treat dataframe with the given options."""
        options_doc = self.options_doc
        for column, option in options_doc['columns'].items():
            self[column] = self[column].treat(option)
