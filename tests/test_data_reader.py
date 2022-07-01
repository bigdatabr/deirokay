import dask.dataframe as dd
import psycopg
import pytest
from pandas import read_csv

from deirokay import data_reader
from deirokay.enums import Backend, DTypes


def test_data_reader_with_json_options():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.json',
        backend=Backend.PANDAS
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_with_yaml_options():

    df = data_reader(
        'tests/transactions_sample.csv',
        options='tests/options.yaml',
        backend=Backend.PANDAS
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_with_dict_options():

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
        backend=Backend.PANDAS
    )
    assert len(df) == 20

    print(df)
    print(df.dtypes)


def test_data_reader_with_dict_options_only_a_few_columns():

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
        backend=Backend.PANDAS
    )
    assert len(df) == 20
    assert len(df.columns) == 2
    assert all(col in ('WERKS01', 'DT_OPERACAO01') for col in df.columns)


def test_data_reader_without_options_exception():
    with pytest.raises(TypeError):
        data_reader(
            'tests/transactions_sample.csv'
        )


def test_data_reader_parquet():
    df = data_reader(
        'tests/sample_parquet.parquet',
        options='tests/sample_parquet.json',
        backend=Backend.PANDAS
    )

    print(df)
    print(df.dtypes)


def test_data_reader_from_dataframe():
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
    df = data_reader(df, options=options, backend=Backend.PANDAS)

    print(df)
    print(df.dtypes)


def test_data_reader_from_dask_dataframe():
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
    df = data_reader(df, options=options, backend=Backend.DASK)

    print(df)
    print(df.dtypes)


@pytest.fixture(scope='module')
def create_db(postgresql_proc):
    db_credentials = {
        'dbname': 'postgres',
        'user': postgresql_proc.user,
        'password': postgresql_proc.password,
        'host': postgresql_proc.host,
        'port': postgresql_proc.port,
        'options': postgresql_proc.options
    }

    with psycopg.connect(**db_credentials) as db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute('''
                CREATE SCHEMA deirokay;
                CREATE TABLE deirokay.test (
                    column1 varchar NULL,
                    column2 int4 NULL,
                    column3 timestamp NULL,
                    column4 float8 NULL,
                    column5 bool NULL
                );
                INSERT INTO deirokay.test
                VALUES
                    ('hey',123,'2021-12-12 20:21:20.000',21.5456,true),
                    ('deirokay',NULL,'2021-12-12 20:21:20.000',21.5456,true),
                    (NULL,8123,'2021-12-12 20:21:20.000',NULL,false),
                    ('ho',1851,'2021-12-04 00:21:20.000',2949.454,NULL);
            ''')
        db_connection.commit()

    yield db_credentials

    with psycopg.connect(**db_credentials) as db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute('DROP SCHEMA deirokay CASCADE;')
        db_connection.commit()


def test_data_reader_from_sql_file(create_db):
    db_credentials = create_db
    options = {
        'columns': {
            'column1': {'dtype': 'string'},
            'column2': {'dtype': 'integer'},
            'column3': {'dtype': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'column4': {'dtype': 'float'},
            'column5': {'dtype': 'boolean'},
        }
    }
    with psycopg.connect(**db_credentials) as con:
        df = data_reader('tests/data_reader_from_sql_file.sql', options,
                         con=con,
                         backend=Backend.PANDAS)

    print(df)
    print(df.dtypes)


def test_data_reader_from_sql_query(create_db):
    db_credentials = create_db
    options = {
        'columns': {
            'column1': {'dtype': 'string'},
            'column2': {'dtype': 'integer'},
            'column3': {'dtype': 'datetime', 'format': '%Y-%m-%d %H:%M:%S'},
            'column4': {'dtype': 'float'},
            'column5': {'dtype': 'boolean'},
        }
    }
    with psycopg.connect(**db_credentials) as con:
        df = data_reader('select * from deirokay.test;', options,
                         sql=True, con=con, backend=Backend.PANDAS)

    print(df)
    print(df.dtypes)
