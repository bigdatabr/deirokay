import pytest
import pandas as pd
from deirokay import data_reader
from deirokay.enums import DTypes


def test_data_reader_from_json():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json'
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_from_dict():

    options = {
        'encoding': 'iso-8859-1',
        'sep': ';',
        'columns': {
            'WERKS01': {'dtype': DTypes.INT64, 'nullable': False,
                        'thousand_sep': '.', 'unique': False},
            'DT_OPERACAO01': {'dtype': DTypes.DATETIME, 'format': '%Y%m%d'},
            'NUM_TRANSACAO01': {'dtype': DTypes.INT64, 'nullable': False,
                                'thousand_sep': '.', 'unique': False},
            'HR_TRANSACAO01': {'dtype': DTypes.TIME, 'format': '%H:%M:%S'},
            'TIPO_PDV': {'dtype': DTypes.STR},
            'PROD_VENDA': {'dtype': DTypes.INT64},
            'COD_MERC_SERV02': {'dtype': DTypes.INT64},
            'COD_SETVENDAS':  {'dtype': DTypes.INT64},
            'NUMERO_PDV_ORIGIN': {'dtype': DTypes.INT64},
            'TIPO_PDV_ORIGIN': {'dtype': DTypes.STR},
            'TIPO_PDV_ORIGIN_GRP': {'dtype': DTypes.STR},
            'QTD_VENDIDA02': {'dtype': DTypes.FLOAT64, 'nullable': False,
                              'thousand_sep': '.', 'decimal_sep': ','},
            'VLR_TOT_VD_ITM02': {'dtype': DTypes.FLOAT64, 'nullable': False,
                                 'thousand_sep': '.', 'decimal_sep': ','},
            'VLR_DESCONTO02': {'dtype': DTypes.FLOAT64, 'nullable': False,
                               'thousand_sep': '.', 'decimal_sep': ','},
            'VLR_LIQUIDO02': {'dtype': DTypes.FLOAT64, 'nullable': False,
                              'thousand_sep': '.', 'decimal_sep': ','},
        }
    }

    df = data_reader(
        'tests/transactions_sample.csv',
        options=options
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_with_bools():
    df = data_reader(
        'tests/sample_with_bools.csv',
        options='tests/sample_with_bools.json'
    )

    print(df)
    print(df.dtypes)


def test_data_reader_without_options_exception():
    with pytest.raises(TypeError):
        data_reader(
            'tests/sample_with_bools.csv'
        )


def test_data_reader_parquet():
    df = data_reader(
        'tests/sample_parquet.parquet',
        options='tests/sample_parquet.json'
    )

    print(df)
    print(df.dtypes)


def test_data_reader_with_invalid_boolean():
    with pytest.raises(ValueError):
        data_reader(
            'tests/sample_with_invalid_bool.csv',
            options='tests/sample_with_bools.json'
        )


def test_data_reader_from_dataframe():
    df = pd.DataFrame({
        'name': ['jorge', 'maria', 'barros'],
        'age': [54, None, 64],
        'married': [True, False, None]
    })
    df = data_reader(df, options='tests/sample_with_bools.json')

    print(df)
    print(df.dtypes)
