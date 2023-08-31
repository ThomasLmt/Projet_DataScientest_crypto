# The code starts a data stream to get live pricing data of ETH markets
# Terminal command : python3 stream.py > results/stream.txt
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    symbol = 'ETH'
    # Connect to the default database postgreSQL
    db_params = {
        'dbname': 'postgres',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

    conn = psycopg2.connect(**db_params)

    # DB creation if binance_stream does not exist
    if not binance_stream_exists(conn, 'binance_stream'):
        binance_stream_db_setup(conn)

    # Stream binance creation for trading pairs ETH

    # Format data

    # Data upload in postgreSQL
    cursor = conn.cursor()
    # Add markets values to the table market

    # Stop the stream

    # Close the connection when done
    cursor.close()
    conn.close()

def binance_stream_exists(conn, db_name):
    # Query the list of existing databases
    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch the result rows
    existing_databases = [row[0] for row in cursor.fetchall()]

    # Check if binance_stream is in the list
    return db_name in existing_databases

def binance_stream_db_setup(conn):

    cursor = conn.cursor()

    # binance_beam DB creation
    cursor.execute("CREATE DATABASE binance_stream")
    conn.commit()
    conn.close()

    # Connexion to teh new database
    db_params = {
        'dbname': 'binance_stream',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Market table creation
    # Record ceation: live source Websocket Binance: Kline/Candlestick Streams
    create_market_table = """
    CREATE TABLE market (
        id INTEGER NOT NULL,
        symbol VARCHAR(20),
        timestamp_close DATETIME,
        price_close FLOAT,
        nbr_trades INTEGER,
        PRIMARY KEY (id)
    );
    """
    cursor.execute(create_market_table)

    # Order table creation (buy and sell)
    # Record creation based on the stream content
    create_order_table = """
    CREATE TABLE order (
        id INTEGER NOT NULL,
        symbol VARCHAR(20),
        type VARCHAR(10),
        amount FLOAT,
        price FLOAT,
        quantity FLOAT,
        date DATETIME,
        PRIMARY KEY (id)
    );
    """
    cursor.execute(create_order_table)

    # Fermeture de la connexion
    conn.commit()
    conn.close()


#def start_stream():
    # Start the stream

#def stop_stream():
    # Stop the stream
    # Close the connection
    #conn.close()

# To automatically launch main from terminal
if __name__ == "__main__":
    main()
