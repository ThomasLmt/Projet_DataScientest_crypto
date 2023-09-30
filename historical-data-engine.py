# Engine to extract all trading pairs crypto market data
# Terminal command : python3 -u historical-data-engine.py > results/engine.txt
# -u allows to follow the process live within engine.txt

import requests
import json
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    """
    The main function that orchestrates the data extraction process.

    It sets up the parameters for data extraction, connects to MongoDB,
    and iterates through trading pairs to fetch and store historical data.

    Returns:
        None
    """
    t0 = time.time()
    print("START - {}\n".format(dt.datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S"),3))

    # Data source used: Binance API url
    url = 'https://api.binance.com/api/v3/klines'

    # Crypto market to study (e.g., BTC)
    money = 'BTC'

    # Time interval between records (e.g., 1M = 1 month, 1h = 1 hour, 1m = 1 minute, 1s = 1 second)
    interval = '1h'
    # interval = '1m'

    # Period for data extraction
    start_period = dt.datetime(2019, 1, 1)
    end_period = dt.datetime(2023, 9, 25)
    # end_period = dt.datetime(2020, 1, 2)

    # List of trading pairs (e.g., all pairs for BTC)
    trading_pairs = get_trading_pairs(money)

    # Connect to MongoDB
    client = connect_mongo()

    # Loop to fetch data for each trading pair
    for pair in trading_pairs:
        print('Market: ', pair)
        engine(url, pair, interval, start_period, end_period, client)

    # Close the MongoDB connection
    client.close()

    tt = (time.time() - t0) / 60
    print("\nRealized in {} minutes".format(round(tt, 3)))
    print("\nEND - {}\n".format(dt.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")))

def connect_mongo():
    """
    Connects to MongoDB and sets up the necessary database and collection.

    Returns:
        MongoClient: A connection to the MongoDB server.
    """
    client = MongoClient(
        host="127.0.0.1",
        port=27017,
        username=os.getenv('MONGO_USER'),
        password=os.getenv('MONGO_PWD')
    )

    # Create or use the 'binance_historical' database
    binance_historical = client['binance_historical']

    # Create or use the 'markets' collection
    markets = binance_historical['markets']

    return client

def get_trading_pairs(money):
    """
    Retrieves trading pairs filtered by the specified base currency.

    Args:
        money (str): The base currency symbol (e.g., 'BTC').

    Returns:
        pandas.Series: A list of trading pairs filtered by the base currency.
    """
    market_list = get_market_list()
    money_trading_pairs = get_pairs(money, market_list)
    return money_trading_pairs

def get_market_list():
    """
    Fetches the list of all available trading pairs from the Binance API.

    Returns:
        List[str]: A list of trading pair symbols.
    """
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)
    data = response.json()

    trading_pairs = []
    for symbol_info in data['symbols']:
        trading_pairs.append(symbol_info['symbol'])

    return trading_pairs

def get_pairs(money, market_list):
    """
    Filters trading pairs based on the specified base currency.

    Args:
        money (str): The base currency symbol (e.g., 'BTC').
        market_list (List[str]): A list of all trading pairs.

    Returns:
        pandas.Series: A filtered list of trading pairs.
    """
    df = pd.DataFrame(market_list)
    df.columns = ['pairs']
    df = df[df.pairs.str.startswith(money)]
    df = df.reset_index()
    return df.pairs

# Data request, data cleaning, data integration in MongoDB
def engine(url, symbol, interval, start_period, end_period, client):
    """
    Fetches historical market data for a specific trading pair.

    Args:
        symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
        interval (str): The time interval between records (e.g., '1h').
        start_period (datetime.datetime): The start date for data extraction.
        end_period (datetime.datetime): The end date for data extraction.
        client (MongoClient): A connection to the MongoDB server.

    Returns:
        None
    """
    # Size of each period of time request to respect API restriction
    day_period = get_chunk(start_period, end_period, interval)

    data = get_data(url, symbol, interval, start_period, end_period, day_period)

    if data:
        df_data = create_df(data, symbol)
        df_clean = clean_df(df_data)

        # Display or use df_clean data as needed
        # print("\n", df_clean.close.describe())
        # print('df_clean.close count: ', df_clean.close.count())

        # Chart
        # df_clean["close"].plot(title='ETHEUR', legend='close')
        # plt.show()

        # MongoDB market data upload
        mongo_upload(df_clean, client)

        # Check
        # Request data from MongoDB to put in a DataFrame
        # markets = client['binance_historical']['markets']
        # df = pd.DataFrame(markets.find({}))
        # print('mongo.close count: ', df.close.count())
        # Check the format of markets records
        # print(markets.find_one())

# API limitation - 1000 records per call
# Number of calls that will be necessary to extract data
def get_chunk(start_period, end_period, interval):
    """
    Calculates the chunk size for data extraction based on the selected time interval.

    Args:
        start_period (datetime.datetime): The start date for data extraction.
        end_period (datetime.datetime): The end date for data extraction.
        interval (str): The time interval between records (e.g., '1h').

    Returns:
        pandas.Timedelta: The size of each time period request.
    """
    time_difference = end_period - start_period
    # Evaluation of the max number of records that could be extracted
    if interval == '1m':
        nbr_records = time_difference.total_seconds() / 60
    else:
        nbr_records = time_difference.total_seconds() / 3600
    # Number of necessary calls to get all records
    nbr_calls = nbr_records / 1000
    # Size period calculation (number of days to get 1000 records)
    return pd.Timedelta(days=time_difference.days / nbr_calls)

def get_data(url, symbol, interval, start_period, end_period, day_period):
    """
    Fetches historical market data from the Binance API.

    Args:
        symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
        interval (str): The time interval between records (e.g., '1h').
        start_period (datetime.datetime): The start date for data extraction.
        end_period (datetime.datetime): The end date for data extraction.
        day_period (pandas.Timedelta): The size of each time period request.

    Returns:
        List[dict]: A list of historical market data.
    """
    data = []
    request_limit_per_second = 20
    # Used to organize minimal sleep to respect API limitation number of requests
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
            # print('cycle: ', cycle)
        # interval 1m
        if cycle == 1 or cycle == 5 or cycle % 10 == 0:
            print('cycle: ', cycle)

        start_period_str = str(int(start_period.timestamp() * 1000))
        end_period_str = str(int((start_period + day_period).timestamp() * 1000))

        par = {'symbol': symbol, 'interval': interval, 'startTime': start_period_str, 'endTime': end_period_str, 'limit': 1000}

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

def create_df(data, symbol):
    """
    Creates a DataFrame from raw historical market data.

    Args:
        data (List[dict]): A list of historical market data.
        symbol (str): The trading pair symbol (e.g., 'BTCUSDT').

    Returns:
        pandas.DataFrame: A DataFrame containing cleaned market data.
    """
    # Create DataFrame
    df_data = pd.DataFrame(data)
    df_data.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume',
                       'close_time', 'qav', 'nbr_trades', 'taker_base_vol',
                       'taker_quote_vol', 'unused_field']

    # Remove 'unused_field'
    df_data.drop(columns=['unused_field'], inplace=True)

    # Convert 'datetime' to datetime format and set as index
    df_data['date'] = [dt.datetime.fromtimestamp(d/1000.0) for d in df_data.datetime]
    df_data.index = df_data['date']
    df_data = df_data.sort_index()
    df_data = df_data.astype({'open': float, 'high': float, 'low': float,
                              'close': float, 'volume': float, 'close_time': float,
                              'qav': float, 'nbr_trades': float, 'taker_base_vol': float,
                              'taker_quote_vol': float})

    # Add 'market' column
    df_data['market'] = symbol

    # Add date related columns
    df_data['year'] = df_data['date'].dt.year
    df_data['month'] = df_data['date'].dt.month
    df_data['day'] = df_data['date'].dt.day
    df_data['day_of_week'] = df_data['date'].dt.dayofweek + 1

    return df_data

def clean_df(df_data):
    """
    Cleans the DataFrame from NaN and zero values.

    Args:
        df_data (pandas.DataFrame): A DataFrame containing market data.

    Returns:
        pandas.DataFrame: A cleaned DataFrame.
    """
    # Check if data is clean
    # df_data.isna().sum()
    # Remove NaN
    df_data = df_data.dropna(axis=1, how='any')
    # Remove zero values
    df_data = df_data[df_data.close != 0]
    return df_data

def mongo_upload(df_clean, client):
    """
    Uploads cleaned market data to MongoDB.

    Args:
        df_clean (pandas.DataFrame): A DataFrame containing cleaned market data.
        client (MongoClient): A connection to the MongoDB server.

    Returns:
        None
    """
    markets = client['binance_historical']['markets']
    # Convert DataFrame to a list of dictionaries
    formatted_data = df_clean.to_dict('records')

    # Insert data into MongoDB
    markets.insert_many(formatted_data)

# Automatically run main when the script is executed
if __name__ == "__main__":
    main()
