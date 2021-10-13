import logging
from datetime import datetime
from typing import Optional, Union

from airflow.models.baseoperator import BaseOperator

import deirokay

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):

    template_fields = ['path_to_file', 'options', 'against', 'save_to']
    template_fields_renderers = {'options': 'json', 'against': 'json'}
    ui_color = '#59f75e'

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
        self.save_to = save_to

    def execute(self, context):
        current_date = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
        df = deirokay.data_reader(self.path_to_file, options=self.options)
        deirokay.validate(df,
                          against=self.against,
                          save_to=self.save_to,
                          current_date=current_date)

        return self.path_to_file
