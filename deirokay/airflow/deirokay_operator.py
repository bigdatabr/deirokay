import logging
from datetime import datetime
from typing import Optional, Union

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator

import deirokay
from deirokay.enums import SeverityLevel
from deirokay.exceptions import ValidationError
from deirokay.validator import raise_validation

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):
    """Parse a DataFrame from a file using Deirokay options and
    validate against a Deirokay Validation Document.
    You may choose different severity levels to trigger a task
    "soft failure" (`skipped` state) or "normal failure" (`failed`
    state)."""

    template_fields = ['path_to_file', 'options', 'against', 'save_to']
    template_fields_renderers = {'options': 'json', 'against': 'json'}
    ui_color = '#59f75e'

    def __init__(
        self,
        path_to_file: str,
        options: Union[dict, str],
        against: Union[dict, str],
        save_to: Optional[str] = None,
        soft_fail_level: int = SeverityLevel.MINIMAL,
        hard_fail_level: int = SeverityLevel.CRITICAL,
        **kwargs
    ):
        """Create an Airflow Deirokay Operator for data validation.

        Parameters
        ----------
        path_to_file : str
            File to be parsed into Deirokay.
        options : Union[dict, str]
            A dict or a local/S3 path to a YAML/JSON options file.
        against : Union[dict, str]
            A dict or a local/S3 path to a YAML/JSON validation
            document file.
        save_to : Optional[str], optional
            Where validation logs will be saved to.
            If None, no log is saved. By default None.
        soft_fail_level : int, optional
            Minimum Deirokay severity level to trigger a
            "soft failure".
            Any statement with lower severity level will only raise a
            warning.
            By default SeverityLevel.MINIMAL (1).
        hard_fail_level : int, optional
            Minimum Deirokay severity level to trigger a task failure.
            By default SeverityLevel.CRITICAL (5).
        """
        super().__init__(**kwargs)

        self.path_to_file = path_to_file
        self.options = options
        self.against = against
        self.save_to = save_to
        self.soft_fail_level = soft_fail_level
        self.hard_fail_level = hard_fail_level

    # docstr-coverage:inherited
    def execute(self, context: dict):
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
            raise_validation(validation_document, SeverityLevel.MINIMAL)
        except ValidationError as e:
            if e.level >= self.hard_fail_level:
                raise AirflowFailException from e
            if e.level >= self.soft_fail_level:
                raise AirflowSkipException from e
