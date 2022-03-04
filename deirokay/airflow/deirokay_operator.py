import logging
import warnings
from datetime import datetime
from typing import Optional, Union

from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator

import deirokay
from deirokay.enums import SeverityLevel
from deirokay.exceptions import ValidationError
from deirokay.validator import raise_validation

logger = logging.getLogger(__name__)


class DeirokayOperator(BaseOperator):
    """Parse a DataFrame from a file using Deirokay options and
    validate against a Deirokay Validation Document.
    You may choose different severity levels to mark a task as
    "soft failure" (`skipped` state) or "normal failure" (`failed`
    state).

    Parameters
    ----------
    data : Optional[str]
        File to be parsed into Deirokay.
    path_to_file : Optional[str]
        (Deprecated) File to be parsed into Deirokay.
        Use `data` instead.
    options : Union[dict, str]
        A dict or a local/S3 path to a YAML/JSON options file.
    against : Union[dict, str]
        A dict or a local/S3 path to a YAML/JSON validation
        document file.
    template : Optional[dict]
        Map of templates to be passed to Deirokay validation.
    save_to : Optional[str], optional
        Where validation logs will be saved to.
        If None, no log is saved. By default None.
    soft_fail_level : Union[SeverityLevel, int]
        Minimum Deirokay severity level to trigger a
        "soft failure".
        Any statement with lower severity level will only raise a
        warning. Set to `None` to never trigger.
        By default SeverityLevel.MINIMAL (1).
    hard_fail_level : Union[SeverityLevel, int]
        Minimum Deirokay severity level to trigger a task failure.
        Set to `None` to never trigger.
        By default SeverityLevel.CRITICAL (5).
    reader_kwargs : Optional[dict]
        Additional keyword arguments for `Deirokay.data_reader` method.
    validator_kwargs : Optional[dict]
        Additional keyword arguments for `Deirokay.validate` method.
    **kwargs : Optional[dict]
        Additional keyword arguments for `BaseOperator`.
    """

    template_fields = [
        'data',
        'options',
        'against',
        'template',
        'save_to',
        'reader_kwargs',
        'validator_kwargs'
    ]
    template_fields_renderers = {'options': 'json', 'against': 'json'}
    ui_color = '#59f75e'

    def __init__(
        self,
        *,
        data: Optional[str] = None,
        path_to_file: Optional[str] = None,  # Deprecated
        options: Union[dict, str],
        against: Union[dict, str],
        template: Optional[dict] = None,
        save_to: Optional[str] = None,
        soft_fail_level: Union[SeverityLevel, int] = SeverityLevel.MINIMAL,
        hard_fail_level: Union[SeverityLevel, int] = SeverityLevel.CRITICAL,
        reader_kwargs: Optional[dict] = None,
        validator_kwargs: Optional[dict] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        assert bool(data) is not bool(path_to_file), (
            'Declare either `data` or `path_to_file`, but not both.'
        )
        if path_to_file:
            warnings.warn(
                'The argument `path_to_file` is deprecated and will be'
                ' removed in next major release. Use `data` instead.',
                DeprecationWarning
            )
        assert options
        assert against
        self.data = data or path_to_file
        self.options = options
        self.against = against
        self.template = template
        self.save_to = save_to
        self.soft_fail_level = soft_fail_level
        self.hard_fail_level = hard_fail_level
        self.reader_kwargs = reader_kwargs or {}
        self.validator_kwargs = validator_kwargs or {}

    # docstr-coverage:inherited
    def execute(self, context: dict):
        current_date = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
        df = deirokay.data_reader(
            self.data,
            options=self.options,
            **self.reader_kwargs
        )

        validation_document = deirokay.validate(
            df,
            against=self.against,
            template=self.template,
            save_to=self.save_to,
            current_date=current_date,
            raise_exception=False,
            **self.validator_kwargs
        )
        try:
            raise_validation(validation_document, SeverityLevel.MINIMAL)
        except ValidationError as e:
            if (
                self.hard_fail_level is not None and
                e.level >= self.hard_fail_level
            ):
                raise e
            if (
                self.soft_fail_level is not None and
                e.level >= self.soft_fail_level
            ):
                raise AirflowSkipException from e
