import os
import time
import yfinance as yf
import dateutil.relativedelta
from datetime import date
import datetime
import numpy as np
import sys
from stocklist import NasdaqController
from tqdm import tqdm
import sqlite3
from sqlite3 import Error
import pandas as pd

from joblib import Parallel, delayed, parallel_backend
import multiprocessing


class mainObj:
    def __init__(self):
        pass

    def create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)
        return conn

    def getData(self, ticker):
        history_exists = False
        currentDate = datetime.datetime.strptime(
            date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
        # if db found, fetch already downloaded data
        try:
            sql_qry = "SELECT * FROM stock_history WHERE symbol = '" + ticker + "' ORDER BY Date ASC"
            df = pd.read_sql_query(sql_qry, self.conn, index_col='Date')
            # if entry exists in DB:
            last_saved_date_time = datetime.datetime.strptime(df.index[-1], '%Y-%m-%d %H:%M:%S')
            df.index = pd.to_datetime(df.index)
            # pastDate = latest entry date + 1
            pastDate = last_saved_date_time + dateutil.relativedelta.relativedelta(days=1)
            history_exists = True
        except Exception as e:
            print (e)
            pastDate = currentDate - dateutil.relativedelta.relativedelta(days=4)
        data = yf.download(ticker, pastDate, currentDate)
        # add symbol to dataframe and database table
        data['symbol'] = ticker
        data.index = data.index.normalize()
        # save fetched data to database table
        data.to_sql('stock_history', self.conn, index=True, index_label='Date', if_exists='append')
        if history_exists:
            data = pd.concat([df, data], ignore_index=False)
        # store what was fetched to DB
        return data[["Volume"]]

    def find_anomalies(self, data, cutoff):
        anomalies = []
        data_std = np.std(data['Volume'])
        data_mean = np.mean(data['Volume'])
        anomaly_cut_off = data_std * cutoff
        upper_limit = data_mean + anomaly_cut_off
        indexs = data[data['Volume'] > upper_limit].index.tolist()
        outliers = data[data['Volume'] > upper_limit].Volume.tolist()
        index_clean = [str(x)[:-9] for x in indexs]
        d = {'Dates': index_clean, 'Volume': outliers}
        return d

    def find_anomalies_two(self, data, cutoff):
        indexs = []
        outliers = []
        data_std = np.std(data['Volume'])
        data_mean = np.mean(data['Volume'])
        anomaly_cut_off = data_std * cutoff
        upper_limit = data_mean + anomaly_cut_off
        data.reset_index(level=0, inplace=True)
        for i in range(len(data)):
            temp = data['Volume'].iloc[i]
            if temp > upper_limit:
                indexs.append(str(data['Date'].iloc[i])[:-9])
                outliers.append(temp)
        d = {'Dates': indexs, 'Volume': outliers}
        return d

    def customPrint(self, d, tick):
        print("\n\n\n*******  " + tick.upper() + "  *******")
        print("Ticker is: "+tick.upper())
        for i in range(len(d['Dates'])):
            str1 = str(d['Dates'][i])
            str2 = str(d['Volume'][i])
            print(str1 + " - " + str2)
        print("*********************\n\n\n")

    def days_between(self, d1, d2):
        d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
        d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)

    def parallel_wrapper(self,x, cutoff, currentDate, positive_scans):
        d = (self.find_anomalies_two(self.getData(x), cutoff))
        if d['Dates']:
            for i in range(len(d['Dates'])):
                if self.days_between(str(currentDate)[:-9], str(d['Dates'][i])) <= 3:
                    self.customPrint(d, x)
                    stonk = dict()
                    stonk['Ticker'] = x
                    stonk['TargetDate'] = d['Dates'][0]
                    stonk['TargetVolume'] = d['Volume'][0]
                    positive_scans.append(stonk)

    def main_func(self, cutoff):
        self.conn = self.create_connection(r'stock_history.db')
        if not self.conn:
            print ('could not create db file')
            exit (0)
        StocksController = NasdaqController(True)
        list_of_tickers = StocksController.getList()
        currentDate = datetime.datetime.strptime(
            date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
        start_time = time.time()

        manager = multiprocessing.Manager()
        positive_scans = manager.list()

        with parallel_backend('loky', n_jobs=multiprocessing.cpu_count()):
            Parallel()(delayed(self.parallel_wrapper)(x, cutoff, currentDate, positive_scans) for x in tqdm(list_of_tickers) )

        print("\n\n\n\n--- this took %s seconds to run ---" %
              (time.time() - start_time))

        return positive_scans

if __name__ == '__main__':
    mainObj().main_func(10)
# input desired anomaly standard deviation cuttoff
# run time around 50 minutes for every single ticker.