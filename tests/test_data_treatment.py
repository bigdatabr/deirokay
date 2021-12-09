import json

import pytest
from numpy import nan
from pandas.testing import assert_series_equal

from deirokay.enums import DTypes
from deirokay.parser import get_dtype_treater, get_treater_instance


@pytest.mark.parametrize('dtype, params, values', [
    (
        DTypes.INTEGER,
        {},
        [45, None, 232, -12, nan]
    ),
    (
        'integer',
        {'thousand_sep': ','},
        ['45,000', None, '232', '-12,125']
    ),
    (
        DTypes.FLOAT,
        {},
        [-1.4, None, 4.2, 1.6e2]
    ),
    (
        'float',
        {'thousand_sep': ',', 'decimal_sep': '.'},
        ['-51,121.4', None, '12,654.2', '12,221.6']
    ),
    (
        DTypes.STRING,
        {},
        ['Apple', '', None, 'Noice']
    ),
    (
        DTypes.DATETIME,
        {'format': '%Y%m%dT%H%M%S'},
        ['20001231T202100', None, '20010101T202100', '20201231T235900']
    ),
    (
        DTypes.DATE,
        {'format': '%Y%m'},
        ['200012', None, '200101', '200112']
    ),
    (
        DTypes.TIME,
        {'format': '%H:%M'},
        ['02:00', None, '17:59', '23:12']
    ),
    (
        DTypes.BOOLEAN,
        {},
        [True, None, False, True]
    ),
    (
        'boolean',
        {'truthies': ['on'], 'falsies': ['off']},
        ['on', 'off', None, True, False]
    )
])
def test_dtype_parsing_for_Python_types(dtype, params, values):
    treater_cls = get_dtype_treater(dtype)
    treater_instance = treater_cls(**params)

    # Use Deirokay to treat Python types
    parsed = treater_instance(values)

    # Serialize with Deirokay
    serialized = json.dumps(treater_cls.serialize(parsed))
    json_parse = json.loads(serialized)

    # Test for replication
    parsed_from_serialized = (
        get_treater_instance(json_parse['parser'])(json_parse['values'])
    )
    print(serialized)
    assert_series_equal(parsed, parsed_from_serialized)
