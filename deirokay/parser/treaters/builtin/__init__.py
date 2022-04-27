from deirokay._utils import recursive_subclass_generator

from .base_treater import BaseTreater
from .boolean_treater import BooleanTreater  # noqa: F401
from .date_treater import DateTreater  # noqa: F401
from .datetime64_treater import DateTime64Treater  # noqa: F401
from .decimal_treater import DecimalTreater  # noqa: F401
from .float_treater import FloatTreater  # noqa: F401
from .integer_treater import IntegerTreater  # noqa: F401
from .numeric_treater import NumericTreater  # noqa: F401
from .string_treater import StringTreater  # noqa: F401
from .time_treater import TimeTreater  # noqa: F401
from .validator import Validator  # noqa: F401

__all__ = tuple(
    cls.__name__
    for cls in recursive_subclass_generator(BaseTreater)
)
