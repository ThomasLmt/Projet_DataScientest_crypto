# The code starts a data stream to get live pricing data of ETH markets
# Terminal command : python3 -u stream.py >> results/stream.txt
import psycopg2
import asyncio
import nest_asyncio
from binance import AsyncClient, BinanceSocketManager
import time
import pandas as pd
import datetime as dt
import os
from dotenv import load_dotenv

load_dotenv()

# Nest the event loop for jupyter tests
nest_asyncio.apply()

# Variable to control the loop period
stop_loop = False

# Params
timeout = 10
interval = '1s'
duration = 20
symbol = 'ETH'
market = 'ETHUSDT'

async def main():
    # Create database if first launch
    postgres_setup()

    # Stream and postgres upload
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client, user_timeout=timeout)
    # start any sockets here, i.e a trade socket
    ks = bm.kline_socket(market, interval=interval)

    # Set the duration to run the loop (in seconds)
    loop_duration = duration  # Change this to the desired duration

    # Get the start time
    start_time = time.time()

    # Initialize an empty DataFrame
    data = []

    db_params = {
        'database': 'binance_stream',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    async with ks as tscm:
        while not stop_loop and time.time() - start_time < loop_duration:
            res = await tscm.recv()

            # Extract relevant data from the received message
            event_timestamp = res['E']
            event_date = dt.datetime.fromtimestamp(event_timestamp / 1000.0)
            start_timestamp = res['k']['t']
            close_timestamp = res['k']['T']
            open_price = float(res['k']['o'])
            high_price = float(res['k']['h'])
            low_price = float(res['k']['l'])
            close_price = float(res['k']['c'])
            volume = float(res['k']['v'])
            qav = float(res['k']['q'])
            nbr_trades = int(res['k']['n'])
            taker_base_vol = float(res['k']['V'])
            taker_quote_vol = float(res['k']['Q'])
            symbol = res['s']

            # Prepare the data in a tuple
            row_data = (
                event_timestamp, event_date, open_price, high_price,
                low_price, close_price, volume, start_timestamp, close_timestamp,
                qav, nbr_trades, taker_base_vol, taker_quote_vol, symbol
            )

            print(row_data)

            # Define the SQL query to insert data into the "market" table
            insert_query = """
            INSERT INTO market (
                event_timestamp, event_date, open, high, low, close, volume,
                start_timestamp, close_timestamp, qav, nbr_trades,
                taker_base_vol, taker_quote_vol, symbol
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """

            # Execute the insert query with the data
            cursor.execute(insert_query, row_data)
            conn.commit()

            print("Kline inserted successfully: ", dt.datetime.now().strftime('%Y-%m-%d'))

    cursor.close()
    conn.close()
    # Close the Binance client
    await client.close_connection()


def postgres_setup():
    # Connect to the default database postgreSQL
    db_params = {
        'database': 'postgres',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

    # DB creation if binance_stream does not exist
    if not binance_stream_exists(db_params):
        binance_stream_db_setup(db_params)


def binance_stream_exists(db_params):
    conn = psycopg2.connect(**db_params)
    # Query the list of existing databases
    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch the result rows
    existing_databases = [row[0] for row in cursor.fetchall()]

    # Close the connection when done
    cursor.close()
    conn.close()

    # Check if binance_stream is in the list
    return 'binance_stream' in existing_databases

def binance_stream_db_setup(db_params):
    # binance_stream DB creation
    conn = psycopg2.connect(**db_params)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE binance_stream")
    conn.close()
    print("Database binance_stream has been created successfully")

    # Connexion to the new database
    db_params = {
        'database': 'binance_stream',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**db_params)
    conn.autocommit = True
    cursor = conn.cursor()

    # Market table creation
    # Record creation: live source Websocket Binance: Kline/Candlestick Streams
    create_market_table = """
    CREATE TABLE market (
        id SERIAL PRIMARY KEY,
        event_timestamp BIGINT,
        event_date TIMESTAMP,
        open DECIMAL,
        high DECIMAL,
        low DECIMAL,
        close DECIMAL,
        volume DECIMAL,
        start_timestamp BIGINT,
        close_timestamp BIGINT,
        qav DECIMAL,
        nbr_trades INTEGER,
        taker_base_vol DECIMAL,
        taker_quote_vol DECIMAL,
        symbol VARCHAR(20)
    );
    """
    cursor.execute(create_market_table)

    # Order table creation (buy and sell)
    # Record creation based on the stream content
    # create_order_table = """
    # CREATE TABLE order (
    #     id INTEGER NOT NULL,
    #     symbol VARCHAR(20),
    #     type VARCHAR(10),
    #     price FLOAT,
    #     quantity FLOAT,
    #     trade_timestamp DATETIME,
    #     trade_date DATETIME,
    #     trade_id INTEGER,
    #     PRIMARY KEY (id)
    # );
    # """
    # cursor.execute(create_order_table)

    print("Table market has been created successfully")

    # Fermeture de la connexion
    conn.close()

# To automatically launch main from terminal
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
