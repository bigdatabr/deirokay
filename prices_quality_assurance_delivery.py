import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

sys.path.append('/home/ubuntu/momo_priceo/src/data_pipelines/')
from data_engineering.athena import AthenaConnection
from database.connection import Connection

class PricesQualityAssuranceDelivery:
    def __init__(self, aggregator, file_to_client):
        """PricesQualityAssuranceDelivery constructor.
        """
        self.aggregator = aggregator
        self._file_to_client = pd.read_excel(file_to_client)
        self.file_to_client = self._file_to_client.copy()
        self.conn = Connection().connect()

    def create_log_file(self):
        """
        """
        # current date and time to name log file
        tz = timezone(timedelta(hours=-3))
        _date_n_time = datetime.now()
        date_n_time = _date_n_time.astimezone(tz).strftime('%Y-%m-%d_%H:%M:%S')
        # create file
        self.log_file_name = f'quality_assurance_delivery_{self.aggregator}_{date_n_time}.txt'
        self.log_file = open(self.log_file_name, 'w')
        self.log_file.write(f'PRICES QUALITY ASSURANCE - MOMO DELIVERY {self.aggregator.upper()}\n')
        self.log_file.close()
        
    def write_log(self, log_message):
        """
        """
        self.log_file = open(self.log_file_name, 'a')
        self.log_file.write(log_message)
        self.log_file.close()

    def check_prices_update(self):
        """
        """
        query_prices = """
                       SELECT *
                       FROM sail_delivery_{aggregator}.item_prices
                       """.format(aggregator=self.aggregator)
        item_prices = pd.read_sql(query_prices, self.conn)

        check_prices = self.file_to_client.merge(item_prices,
                                                 left_on=['PRICE', 'BKNUMBER', 'COD_SETVENDAS'],
                                                 right_on=['base_price', 'customer_id', 'item_id'],
                                                 how='left')

        if check_prices[check_prices.base_price == check_prices.PRICE].shape[0] > 0:
            log_message = '[2] ATUALIZACAO DE PRECOS: VERIFICAR\n'
        if check_prices[check_prices.base_price == check_prices.PRICE].shape[0] == 0:
            log_message = '[2] ATUALIZACAO DE PRECOS: OK\n'

        # write log
        print(log_message)
        self.write_log(log_message)         
