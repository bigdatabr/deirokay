import dask.dataframe as dd
import pytest
from pandas import read_csv

from deirokay import data_reader
from deirokay.enums import DTypes


def test_data_reader_with_json_options_dask():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend='dask'
    )
    assert len(df.compute()) == 20

    print(df.compute())
    print(df.dtypes)


def test_data_reader_with_yaml_options_dask():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.yaml',
        backend='dask'
    )
    assert len(df.compute()) == 20

    print(df.compute())
    print(df.dtypes)


def test_data_reader_with_dict_options_dask():

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
            'VLR_LIQUIDO02': {'dtype': DTypes.DECIMAL, 'nullable': True,
                              'thousand_sep': '.', 'decimal_sep': ','},
            'ACTIVE': {'dtype': DTypes.BOOL, 'truthies': ['active'],
                       'falsies': ['inactive']},
        }
    }

    df = data_reader(
        'tests/transactions_sample.csv',
        options=options,
        backend='dask'
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_with_dict_options_only_a_few_columns_dask():

    options = {
        'encoding': 'iso-8859-1',
        'sep': ';',
        'columns': {
            'WERKS01': {'dtype': DTypes.INT64, 'nullable': False,
                        'thousand_sep': '.', 'unique': False},
            'DT_OPERACAO01': {'dtype': DTypes.DATETIME, 'format': '%Y%m%d'}
        }
    }

    df = data_reader(
        'tests/transactions_sample.csv',
        options=options,
        backend='dask'
    )
    assert len(df) == 20
    assert len(df.columns) == 2
    assert all(col in ('WERKS01', 'DT_OPERACAO01') for col in df.columns)


def test_data_reader_without_options_exception_dask():
    with pytest.raises(TypeError):
        data_reader(
            'tests/transactions_sample.csv',
            backend='dask'
        )


def test_data_reader_parquet_dask():
    df = data_reader(
        'tests/sample_parquet.parquet',
        options='tests/sample_parquet.json',
        backend='dask'
    )

    print(df)
    print(df.dtypes)


def test_data_reader_from_dataframe_dask():
    df = read_csv('tests/transactions_sample.csv', sep=';',
                  thousands='.', decimal=',')

    options = {
        'encoding': 'iso-8859-1',
        'sep': ';',
        'columns': {
            'WERKS01': {'dtype': DTypes.INT64, 'nullable': False,
                        'unique': False},
            'DT_OPERACAO01': {'dtype': DTypes.DATETIME, 'format': '%Y%m%d'},
            'NUM_TRANSACAO01': {'dtype': DTypes.INT64, 'nullable': False,
                                'unique': False},
            'HR_TRANSACAO01': {'dtype': DTypes.TIME, 'format': '%H:%M:%S'},
            'TIPO_PDV': {'dtype': DTypes.STR},
            'PROD_VENDA': {'dtype': DTypes.INT64},
            'COD_MERC_SERV02': {'dtype': DTypes.INT64},
            'COD_SETVENDAS':  {'dtype': DTypes.INT64},
            'NUMERO_PDV_ORIGIN': {'dtype': DTypes.INT64},
            'TIPO_PDV_ORIGIN': {'dtype': DTypes.STR},
            'TIPO_PDV_ORIGIN_GRP': {'dtype': DTypes.STR},
            'QTD_VENDIDA02': {'dtype': DTypes.INT64, 'nullable': False},
            'VLR_TOT_VD_ITM02': {'dtype': DTypes.FLOAT64, 'nullable': False},
            'VLR_DESCONTO02': {'dtype': DTypes.FLOAT64, 'nullable': False},
            'VLR_LIQUIDO02': {'dtype': DTypes.DECIMAL, 'nullable': True},
            'ACTIVE': {'dtype': DTypes.BOOL, 'truthies': ['active'],
                       'falsies': ['inactive']},
        }
    }
    df = data_reader(df, options=options, backend='dask')

    print(df)
    print(df.dtypes)


def test_data_reader_from_dask_dataframe_dask():
    df = read_csv('tests/transactions_sample.csv', sep=';',
                  thousands='.', decimal=',')
    df = dd.from_pandas(df, npartitions=1)

    options = {
        'encoding': 'iso-8859-1',
        'sep': ';',
        'columns': {
            'WERKS01': {'dtype': DTypes.INT64, 'nullable': False,
                        'unique': False},
            'DT_OPERACAO01': {'dtype': DTypes.DATETIME, 'format': '%Y%m%d'},
            'NUM_TRANSACAO01': {'dtype': DTypes.INT64, 'nullable': False,
                                'unique': False},
            'HR_TRANSACAO01': {'dtype': DTypes.TIME, 'format': '%H:%M:%S'},
            'TIPO_PDV': {'dtype': DTypes.STR},
            'PROD_VENDA': {'dtype': DTypes.INT64},
            'COD_MERC_SERV02': {'dtype': DTypes.INT64},
            'COD_SETVENDAS':  {'dtype': DTypes.INT64},
            'NUMERO_PDV_ORIGIN': {'dtype': DTypes.INT64},
            'TIPO_PDV_ORIGIN': {'dtype': DTypes.STR},
            'TIPO_PDV_ORIGIN_GRP': {'dtype': DTypes.STR},
            'QTD_VENDIDA02': {'dtype': DTypes.INT64, 'nullable': False},
            'VLR_TOT_VD_ITM02': {'dtype': DTypes.FLOAT64, 'nullable': False},
            'VLR_DESCONTO02': {'dtype': DTypes.FLOAT64, 'nullable': False},
            'VLR_LIQUIDO02': {'dtype': DTypes.DECIMAL, 'nullable': True},
            'ACTIVE': {'dtype': DTypes.BOOL, 'truthies': ['active'],
                       'falsies': ['inactive']},
        }
    }
    df = data_reader(df, options=options, backend='dask')

    print(df)
    print(df.dtypes)


def test_data_reader_from_sql_file_dask():
    options = {
        'columns': {
            'column1': {'dtype': 'string'},
            'column2': {'dtype': 'integer'},
            'column3': {'dtype': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'column4': {'dtype': 'float'},
            'column5': {'dtype': 'boolean'},
        }
    }
    with pytest.raises(NotImplementedError):
        data_reader('tests/data_reader_from_sql_file.sql',
                    options,
                    backend='dask')


def test_data_reader_from_sql_query_dask():
    options = {
        'columns': {
            'column1': {'dtype': 'string'},
            'column2': {'dtype': 'integer'},
            'column3': {'dtype': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'column4': {'dtype': 'float'},
            'column5': {'dtype': 'boolean'},
        }
    }
    with pytest.raises(NotImplementedError):
        data_reader('select * from deirokay.test;', options,
                    sql=True, backend='dask')
