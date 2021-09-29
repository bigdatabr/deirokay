import logging
from typing import Optional

import deirokay
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):

    ui_color = '#1d3e63'

    def __init__(
        self,
        path_to_file: str,
        deirokay_options_json: str,
        deirokay_assertions_json: str,
        save_json: Optional[str] = None,
        pandas_kwargs: Optional[dict] = {},
        **kwargs
    ):
        super().__init__(**kwargs)

        self.path_to_file = path_to_file
        self.deirokay_options_json = deirokay_options_json
        self.deirokay_assertions_json = deirokay_assertions_json
        self.pandas_kwargs = pandas_kwargs

    def execute(self, context):
        df = deirokay.data_reader(self.path_to_file,
                                  options_json=self.deirokay_options_json,
                                  **self.pandas_kwargs)
        deirokay.validate(df, against_json=self.deirokay_assertions_json)
