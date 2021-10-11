import logging
from typing import Optional, Union

from airflow.models.baseoperator import BaseOperator

import deirokay

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):

    ui_color = '#1d3e63'

    def __init__(
        self,
        path_to_file: str,
        options: Union[dict, str],
        against: Union[dict, str],
        save_to: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.path_to_file = path_to_file
        self.options = options
        self.against = against

    def execute(self, context):
        df = deirokay.data_reader(self.path_to_file, options=self.options)
        deirokay.validate(df, against=self.against)

        return self.path_to_file
