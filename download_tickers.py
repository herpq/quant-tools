import pandas as pd
import numpy as np
import json as json
import csv
import os
import requests
import datetime as dt
from contextlib import closing
import asyncio
from timeit import default_timer
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import sys
from dotenv import load_dotenv
load_dotenv()

UNIVERSE_FILE_NAME = "./Data/all_pairs.csv"

TIINGO_TOKEN = os.environ['TIINGO_TOKEN']

def download_all_pairs():
    headers = {
        'Content-Type': 'application/json'
    }
    metadata_url = "https://api.tiingo.com/tiingo/crypto"
    final_url = "{}?token={}".format(metadata_url, TIINGO_TOKEN)

    with requests.Session() as s:
        download = s.get(final_url, headers=headers)

        decoded_content = download.content.decode('utf-8')

        # BUG PATCH to read data temporarily
        df = pd.read_json(decoded_content)
        df = df[['ticker','baseCurrency','name','quoteCurrency']]
        df.to_csv("./Data/all_pairs.csv", index=False)

#         cr = csv.reader(decoded_content.splitlines(), delimiter=',')
#         my_list = list(cr)

#         with open(UNIVERSE_FILE_NAME, mode='w+') as file:
#             file_writer = csv.writer(
#                 file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#             for row in my_list:
#                 file_writer.writerow(row)

    print("Done downloading all tickers")


def load_valid_pairs():
    download_all_pairs()
    all_pairs = pd.read_csv(UNIVERSE_FILE_NAME)
    all_pairs = all_pairs[all_pairs['quoteCurrency'] == 'usd']
    all_pairs = all_pairs[all_pairs['ticker'].notna()]
    all_pairs = all_pairs[all_pairs['baseCurrency'].notna()]
    return all_pairs['ticker']


# EXCHANGE = 'bitfinex' # can toggle here for a particular exchange

def save_prices_for_tickers(tickers, session, freq, startDate='2014-01-01', endDate='2040-01-01'):
    headers = {
        'Content-Type': 'application/json'
    }
    # csv not supported yet
    request_string = 'https://api.tiingo.com/tiingo/crypto/prices?tickers={}&startDate={}&endDate={}&resampleFreq={}&token={}'
    with closing(session.get(request_string.format(tickers, startDate, endDate, freq, TIINGO_TOKEN), headers=headers, stream=True)) as r:
        print("Downloading...")

        json_response = r.json()
        
        if freq == '1day':
            directory = './Data/Prices/daily/csv'
        if freq == '60m':
            directory = './Data/Prices/hourly/csv'

        if not os.path.exists(directory):
            os.makedirs(directory)
        for ticker_data in json_response:
            ticker = ticker_data["ticker"]
            price_data = ticker_data["priceData"]
            
            
            with open('{}/{}.csv'.format(directory, ticker), mode='w+') as file:
                file_writer = csv.writer(
                    file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in price_data:
                    csv_row = [row['date'], row['open'], row['high'],
                               row['low'], row['close'], row['volume']]
                    file_writer.writerow(csv_row)

            if len(price_data) > 0:
                print("Done: {}".format(ticker))
            else:
                print("No rows for: {}".format(ticker))


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def download_data(freq):
    starttime = default_timer()
    print("Starting download jobs:{}".format(starttime))

    all_tickers = load_valid_pairs()
    batched_tickers = chunks(list(all_tickers), 80)
    yesterday = str(dt.date.today() - dt.timedelta(1))
#         with ThreadPoolExecutor(max_workers=10) as executor:
    with ThreadPoolExecutor() as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()

            tasks = [
                loop.run_in_executor(
                    executor,
                    save_prices_for_tickers,
                    # Allows us to pass in multiple arguments to `fetch`
                    *(",".join(tickers), session, freq, '2014-01-01', yesterday)
                )
                for tickers in batched_tickers
            ]

            # Initializes the tasks to run and awaits their results
            for response in await asyncio.gather(*tasks):
                print("Finished all downloads: {}".format(
                    default_timer() - starttime))

                print("Creating agg file...")
                final_df = None

                if freq == '1day':
                    directory = './Data/Prices/daily/csv'
                if freq == '60m':
                    directory = './Data/Prices/hourly/csv'
                    
                ticker_files = [
                    "{}/{}.csv".format(directory, ticker) for ticker in all_tickers]
                ticker_files = [
                    ticker_file for ticker_file in ticker_files if os.path.exists(ticker_file)]

                dfs = []
                header = ["as_of_date", "open",
                          "high", "low", "close", "volume"]
                for ticker_file in ticker_files:
                    df = pd.read_csv(ticker_file, names=header)
                    if df.empty:
                        continue
                    df['ticker'] = ticker_file.split(
                        "/")[-1].replace(".csv", "")
                    dfs.append(df)

                final_df = pd.concat(dfs, ignore_index=True)
                final_df['as_of_date'] = pd.to_datetime(
                    final_df['as_of_date'])
                if final_df is not None:
                    print("Final df preparing")
                    final_df.to_csv("{}/agg_prices.csv".format(directory), index=False)
                print("Finished df prepared {}".format(
                    default_timer() - starttime))

                # Prevent running multiple threads. Break at first response completion
                break


def run_async_download(freq='1day'):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(download_data(freq))
    loop.run_until_complete(future)


if __name__ == "__main__":
    run_async_download()