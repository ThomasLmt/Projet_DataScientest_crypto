# The code starts a data stream to get live pricing data of BTC markets
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
duration = 59 # Generates a stream of 59s
symbol = 'BTC'
symbols = ['BTCEUR', 'BTCDAI', 'BTCGBP', 'BTCUSDC']

async def main():
    # Create database if first launch
    postgres_setup()

    # Stream and postgres upload
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client, user_timeout=timeout)

    db_params = {
        'database': 'binance_stream',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Run the streams concurrently
    tasks = [stream_market(market, bm, conn, cursor) for market in symbols]
    await asyncio.gather(*tasks)

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

    print("Table market has been created successfully")

    create_markets_7d_data_table = """
    CREATE TABLE markets_7d_data (
        id SERIAL PRIMARY KEY,
        start_timestamp BIGINT,
        day INTEGER,
        month INTEGER,
        year INTEGER,
        day_of_week INTEGER,
        btceur_close DECIMAL,
        btceur_open DECIMAL,
        btceur_high DECIMAL,
        btceur_low DECIMAL,
        btceur_rolling_avg_7d DECIMAL,
        btceur_nbr_trades INTEGER,
        btceur_volume DECIMAL,
        btcdai_close DECIMAL,
        btcdai_open DECIMAL,
        btcdai_high DECIMAL,
        btcdai_low DECIMAL,
        btcdai_rolling_avg_7d DECIMAL,
        btcdai_nbr_trades INTEGER,
        btcdai_volume DECIMAL,
        btcgbp_close DECIMAL,
        btcgbp_open DECIMAL,
        btcgbp_high DECIMAL,
        btcgbp_low DECIMAL,
        btcgbp_rolling_avg_7d DECIMAL,
        btcgbp_nbr_trades INTEGER,
        btcgbp_volume DECIMAL,
        btcusdc_close DECIMAL,
        btcusdc_open DECIMAL,
        btcusdc_high DECIMAL,
        btcusdc_low DECIMAL,
        btcusdc_rolling_avg_7d DECIMAL,
        btcusdc_nbr_trades INTEGER,
        btcusdc_volume DECIMAL

    );
    """
    cursor.execute(create_markets_7d_data_table)

    print("Table markets_7d_data has been created successfully")

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

    #print("Table market has been created successfully")

    # Fermeture de la connexion
    conn.close()

async def stream_market(market, bm, conn, cursor):
    # start any sockets here, i.e a trade socket
    ks = bm.kline_socket(market, interval=interval)

    # Set the duration to run the loop (in seconds)
    loop_duration = duration  # Change this to the desired duration

    # Get the start time
    start_time = time.time()

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

            print("Klines inserted successfully: ", dt.datetime.now().strftime('%Y-%m-%d'))

            # Check if the market is BTCEUR to populate markets_7d_data
            if market == 'BTCEUR':
                # Calculate rolling 7-day average for BTCEUR
                rolling_avg_7d_btceur = calculate_rolling_avg(cursor, event_timestamp, 'BTCEUR')

                # Prepare the data in a tuple for markets_7d_data table
                row_data_7d = (
                    event_timestamp, event_date.day, event_date.month, event_date.year, event_date.weekday(),
                    close_price, float(res['k']['o']), float(res['k']['h']), float(res['k']['l']), rolling_avg_7d_btceur,
                    nbr_trades, volume, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0
                )

                print(row_data_7d)

                # Define the SQL query to insert data into the "markets_7d_data" table
                insert_query_7d_data = """
                INSERT INTO markets_7d_data (
                    start_timestamp, day, month, year, day_of_week,
                    btceur_close, btceur_open, btceur_high, btceur_low, btceur_rolling_avg_7d,
                    btceur_nbr_trades, btceur_volume,
                    btcdai_close, btcdai_open, btcdai_high, btcdai_low, btcdai_rolling_avg_7d,
                    btcdai_nbr_trades, btcdai_volume,
                    btcgbp_close, btcgbp_open, btcgbp_high, btcgbp_low, btcgbp_rolling_avg_7d,
                    btcgbp_nbr_trades, btcgbp_volume,
                    btcusdc_close, btcusdc_open, btcusdc_high, btcusdc_low, btcusdc_rolling_avg_7d,
                    btcusdc_nbr_trades, btcusdc_volume
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
                """

                # Execute the insert query with the data for markets_7d_data table
                cursor.execute(insert_query_7d_data, row_data_7d)
                conn.commit()

            # Update records for other markets using CASE statement
            else:
                # Determine the appropriate columns based on the market
                update_columns = {
                    'BTCDAI': ['btcdai_close', 'btcdai_open', 'btcdai_high', 'btcdai_low', 'btcdai_rolling_avg_7d', 'btcdai_nbr_trades', 'btcdai_volume'],
                    'BTCUSDC': ['btcusdc_close', 'btcusdc_open', 'btcusdc_high', 'btcusdc_low', 'btcusdc_rolling_avg_7d', 'btcusdc_nbr_trades', 'btcusdc_volume'],
                    'BTCGBP': ['btcgbp_close', 'btcgbp_open', 'btcgbp_high', 'btcgbp_low', 'btcgbp_rolling_avg_7d', 'btcgbp_nbr_trades', 'btcgbp_volume']
                }

                # Calculate rolling 7-day average for market
                rolling_avg_7d_market = calculate_rolling_avg(cursor, event_timestamp, market)

                # Extend the update_columns for the market to include rolling_avg_7d_market
                #update_columns[market].append(f'{market.lower()}_rolling_avg_7d')

                # Construct the SET clause of the SQL query based on the market
                set_clause = ', '.join([f'{col} = %s' for col in update_columns[market]])

                cursor.execute("SELECT COUNT(*) FROM markets_7d_data WHERE start_timestamp = %s", (event_timestamp,))
                record_exists = cursor.fetchone()[0] > 0

                if record_exists:
                    print('exist')
                    # Construct the SQL query for updating the appropriate columns
                    update_query = f"""
                    UPDATE markets_7d_data
                    SET
                        {set_clause}
                    WHERE
                        start_timestamp = %s;
                    """

                    # Prepare the data for the update
                    update_data = (
                        close_price, float(res['k']['o']), float(res['k']['h']), float(res['k']['l']),
                        rolling_avg_7d_market, nbr_trades, volume, event_timestamp
                    )

                    # Execute the update query with the data for the specific market
                    cursor.execute(update_query, update_data)
                    conn.commit()
                else:
                    print('Does not exist')
                    pass

def calculate_rolling_avg(cursor, start_timestamp, market):
    # Calculate rolling 7-day average for the given market
    query = f"""
    SELECT AVG(close) OVER (ORDER BY event_date RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) AS rolling_avg_7d
    FROM market
    WHERE event_timestamp = {start_timestamp} AND symbol = '{market}';
    """

    cursor.execute(query)
    rolling_avg_7d = cursor.fetchone()[0]
    return rolling_avg_7d

# To automatically launch main from terminal
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
