# Engine to extract all trading pairs crypto market data
# Terminal command : python3 -u historical-data-engine.py > results/engine.txt
# -u allow to follow the process live within engine.txt

import requests
import json
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import time

def main():
    t0 = time.time()
    print("START - {}\n".format(dt.datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S"),3))
    # Binance API url
    url = 'https://api.binance.com/api/v3/klines'

    # interval between records: 1M = 1 month, 1h = 1 hour, 1m = 1 minute, 1s = 1 second
    interval = '1h'

    # Period
    start_period = dt.datetime(2020, 1, 1)
    #end_period = dt.datetime(2023, 8, 23)
    end_period = dt.datetime(2020, 1, 31)

    money = 'ETH'

    # Trading pairs ETH list
    trading_pairs = get_trading_pairs(money)

    # loop to get data for each trading pair
    for pair in trading_pairs:
        print('Market: ',pair)
        engine(url, pair, interval, start_period, end_period)

    tt = (time.time() - t0) / 60
    print("\nRealized in {} minutes".format(round(tt,3)))
    print("\nEND - {}\n".format(dt.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")))

def get_trading_pairs(money):
    market_list = get_market_list()
    money_trading_pairs = get_pairs(money, market_list)
    return money_trading_pairs

def get_market_list():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)
    data = response.json()

    trading_pairs = []
    for symbol_info in data['symbols']:
        trading_pairs.append(symbol_info['symbol'])

    return trading_pairs

def get_pairs(money, market_list):
    df = pd.DataFrame(market_list)
    df.columns = ['pairs']
    df = df[df.pairs.str.startswith(money)]
    df = df.reset_index()
    return df.pairs

# data request, data cleaning, data integration in MongoDB
def engine(url, symbol, interval, start_period, end_period):

    # Size of each period of time request to respect API restriction
    day_period = get_chunk(start_period, end_period, interval)

    data = get_data(url, symbol, interval, start_period, end_period, day_period)

    if data:
        df_data = create_df(data)
        df_clean = clean_df(df_data)
        # Display or use data_df as needed
        print("\n",df_clean.close.describe())
        # Chart
        # df_clean["close"].plot(title = 'ETHEUR', legend = 'close')
        # plt.show()

        # integration mongodb
        # Write function

# API limitation - 500 records per call
# Number of calls that will be necessary to extract data
def get_chunk(start_period, end_period, interval):
    time_difference = end_period - start_period
    # Evaluation of the max number of records that could be extracted
    if interval == '1m':
        nbr_records = time_difference.total_seconds() / 60
    else:
        nbr_records = time_difference.total_seconds() / 3600
    # Number of necessary calls to get all records
    nbr_calls = nbr_records/500
    # size period calculation (nbr of days to get 500 records)
    return pd.Timedelta(days=time_difference.days / nbr_calls)

def get_data(url, symbol, interval, start_period, end_period, day_period):
    data = []
    request_limit_per_second = 20
    # Used to organize minimal sleep to respect API limitation nbr requests
    time_interval = pd.Timedelta(seconds=1 / request_limit_per_second).total_seconds()
    cycle = 1

    while start_period < end_period:
        # Calculate the remaining time between start_period and end_period
        remaining_time = end_period - start_period

        # Adjust the day_period for the last chunk based on the remaining time
        if remaining_time < day_period:
            day_period = remaining_time

        # Sleep to respect API rate limit
        if cycle % 20 == 0:
            time.sleep(time_interval)
            # interval 1h
            # print('cycle: ',cycle)
        # interval 1m
        if cycle == 1 or cycle == 10 or cycle % 800 == 0:
            print('cycle: ',cycle)

        start_period_str = str(int(start_period.timestamp() * 1000))
        end_period_str = str(int((start_period + day_period).timestamp() * 1000))

        par = {'symbol': symbol, 'interval': interval, 'startTime': start_period_str, 'endTime': end_period_str}

        try:
            response = requests.get(url, params=par)
            response.raise_for_status()  # Raise an exception if response status code is not 2xx
            data.extend(json.loads(response.text))
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")

        # Move to the next time interval
        start_period += day_period + pd.Timedelta(days=1)
        cycle += 1

    return data

def create_df(data):
    # Create DataFrame
    df_data = pd.DataFrame(data)
    df_data.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume','close_time', 'qav', 'nbr_trades','taker_base_vol', 'taker_quote_vol', 'unused_field']
    df_data.index = [dt.datetime.fromtimestamp(d/1000.0) for d in df_data.datetime]
    df_data = df_data.sort_index()
    return df_data.astype(float)

def clean_df(df_data):
    # Check if data is clean
    # df_data.isna().sum()
    # Remove NaN
    df_data = df_data.dropna(axis = 1,how='any')
    # Remove 0
    df_data = df_data[df_data.close != 0]
    return df_data

# To automatically launch main from terminal
if __name__ == "__main__":
    main()
