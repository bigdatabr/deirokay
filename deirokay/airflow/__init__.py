"""
Airflow extension using Deirokay.
"""

from .deirokay_operator import DeirokayOperator

__all__ = (
    'DeirokayOperator',
)
