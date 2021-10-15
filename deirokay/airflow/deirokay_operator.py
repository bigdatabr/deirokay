import logging
from datetime import datetime
from typing import Optional, Union

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator

import deirokay
from deirokay.enums import Level
from deirokay.exceptions import ValidationError
from deirokay.validator import raise_validation

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
        soft_fail_level: int = Level.WARNING,
        hard_fail_level: int = Level.CRITICAL,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.path_to_file = path_to_file
        self.options = options
        self.against = against
        self.save_to = save_to
        self.soft_fail_level = soft_fail_level
        self.hard_fail_level = hard_fail_level

    def execute(self, context):
        current_date = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
        df = deirokay.data_reader(self.path_to_file, options=self.options)

        validation_document = deirokay.validate(
            df,
            against=self.against,
            save_to=self.save_to,
            current_date=current_date,
            raise_exception=False
        )
        try:
            raise_validation(validation_document, Level.MINIMAL)
        except ValidationError as e:
            if e.level >= self.hard_fail_level:
                raise AirflowFailException from e
            if e.level >= self.soft_fail_level:
                raise AirflowSkipException from e

        return self.path_to_file
