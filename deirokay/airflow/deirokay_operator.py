import logging
from typing import Optional, Union

import deirokay
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):

    ui_color = '#1d3e63'

    def __init__(
        self,
        path_to_file: str,
        deirokay_options: Union[dict, str],
        deirokay_assertions: Union[dict, str],
        save_to: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.path_to_file = path_to_file
        self.deirokay_options = deirokay_options
        self.deirokay_assertions = deirokay_assertions

    def execute(self, context):
        df = deirokay.data_reader(self.path_to_file,
                                  options=self.deirokay_options)
        deirokay.validate(df, against=self.deirokay_assertions)

        return self.path_to_file
