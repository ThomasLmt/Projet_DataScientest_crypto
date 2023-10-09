# The code uses data stream of table markets_7d_data to generate 7 days average data
# Call bot_api and given the prediction buy BTC
# Terminal command : python3 -u call_bot_api.py >> results/call_bot_api.json
import datetime
import psycopg2
import os
import requests
from binance import Client
from binance.enums import *
import math
from dotenv import load_dotenv

load_dotenv()

# Define the API address and port
api_address = '0.0.0.0' #'54.75.106.132'
api_port = 8000
# Get the BTCUSDT_LIMIT defined to trade
btcusdt_limit = float(os.getenv('BTCUSDT_LIMIT'))

def main():
    params = get_params()
    prediction = call_api(params)
    # Check if the context is a good opportunity to invest in BTC
    # Meaning if prediction is less than the BTCUSDT_LIMIT
    if prediction < btcusdt_limit and prediction != 0:
        print('Buy BTC with USDT')
        # Generate the trade on Binance API
        create_order()
    else:
        print('Prediction is above the BTCUSDT_LIMIT')

def get_params():

    # Connect to your PostgreSQL database
    db_params = {
            'database': 'binance_stream',
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': 'localhost',
            'port': '5432'
        }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Get the 7 days period
    end_date = datetime.datetime.now().date() + datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=6)  # 6 days ago to include a 7-day range

    # Combine the start and end dates with a specific time (midnight)
    start_datetime = datetime.datetime.combine(start_date, datetime.time()).timestamp() * 1000
    end_datetime = datetime.datetime.combine(end_date, datetime.time()).timestamp() * 1000

    # Construct the SQL query to retrieve the nedeed params
    query = """
        SELECT
            AVG(CASE WHEN btceur_close != 0 THEN btceur_close ELSE NULL END) as btceur_close_avg,
            AVG(btceur_open) as btceur_open_avg,
            AVG(btceur_high) as btceur_high_avg,
            AVG(btceur_low) as btceur_low_avg,
            AVG(btceur_rolling_avg_7d) as btceur_rolling_avg_7d_avg,
            AVG(btceur_nbr_trades) as btceur_nbr_trades_avg,
            AVG(btceur_volume) as btceur_volume_avg,
            AVG(CASE WHEN btcdai_close != 0 THEN btcdai_close ELSE NULL END) as btcdai_close_avg,
            AVG(btcdai_open) as btcdai_open_avg,
            AVG(btcdai_high) as btcdai_high_avg,
            AVG(btcdai_low) as btcdai_low_avg,
            AVG(btcdai_rolling_avg_7d) as btcdai_rolling_avg_7d_avg,
            AVG(btcdai_nbr_trades) as btcdai_nbr_trades_avg,
            AVG(btcdai_volume) as btcdai_volume_avg,
            AVG(CASE WHEN btcgbp_close != 0 THEN btcgbp_close ELSE NULL END) as btcgbp_close_avg,
            AVG(btcgbp_open) as btcgbp_open_avg,
            AVG(btcgbp_high) as btcgbp_high_avg,
            AVG(btcgbp_low) as btcgbp_low_avg,
            AVG(btcgbp_rolling_avg_7d) as btcgbp_rolling_avg_7d_avg,
            AVG(btcgbp_nbr_trades) as btcgbp_nbr_trades_avg,
            AVG(btcgbp_volume) as btcgbp_volume_avg,
            AVG(CASE WHEN btcusdc_close != 0 THEN btcusdc_close ELSE NULL END) as btcusdc_close_avg,
            AVG(btcusdc_open) as btcusdc_open_avg,
            AVG(btcusdc_high) as btcusdc_high_avg,
            AVG(btcusdc_low) as btcusdc_low_avg,
            AVG(btcusdc_rolling_avg_7d) as btcusdc_rolling_avg_7d_avg,
            AVG(btcusdc_nbr_trades) as btcusdc_nbr_trades_avg,
            AVG(btcusdc_volume) as btcusdc_volume_avg

        FROM markets_7d_data
        WHERE start_timestamp >= %s AND start_timestamp <= %s
            AND (btceur_close != 0 OR btcdai_close != 0 OR btcgbp_close != 0 OR btcusdc_close != 0)
    """

    # Execute the SQL query with the date range as parameters
    cursor.execute(query, (start_datetime, end_datetime))
    result = cursor.fetchone()

    # Prepare the params to cal bot_api
    params = {
        'day': end_date.day,
        'month': end_date.month,
        'year': end_date.year,
        'day_of_week': end_date.weekday(),
        'btceur_close': result[0],
        'btceur_open': result[1],
        'btceur_high': result[2],
        'btceur_low': result[3],
        'btceur_rolling_avg_7d': result[4],
        'btceur_nbr_trades': result[5],
        'btceur_volume': result[6],
        'btcdai_close': result[7],
        'btcdai_open': result[8],
        'btcdai_high': result[9],
        'btcdai_low': result[10],
        'btcdai_rolling_avg_7d': result[11],
        'btcdai_nbr_trades': result[12],
        'btcdai_volume': result[13],
        'btcgbp_close': result[14],
        'btcgbp_open': result[15],
        'btcgbp_high': result[16],
        'btcgbp_low': result[17],
        'btcgbp_rolling_avg_7d': result[18],
        'btcgbp_nbr_trades': result[19],
        'btcgbp_volume': result[20],
        'btcusdc_close': result[21],
        'btcusdc_open': result[22],
        'btcusdc_high': result[23],
        'btcusdc_low': result[24],
        'btcusdc_rolling_avg_7d': result[25],
        'btcusdc_nbr_trades': result[26],
        'btcusdc_volume': result[27]
    }

    # Print the params
    #print(params)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return params

def call_api(params):
    # Request the prediction
    r = requests.get(f'http://{api_address}:{api_port}/prediction', params=params)

    # Check if the request was successful
    if r.status_code == 200:
        # Convert the prediction value to a float
        prediction = float(r.json())
        print(prediction)

    else:
        print(f'Error: {r.status_code}')
        prediction = 0
    return prediction

def create_order():
    client = Client(api_key=os.getenv('API_KEY_TESTNET'), api_secret=os.getenv('API_SECRET_TESTNET'), testnet=True)

    # # Get the account information
    account_info = client.get_account()

    # Balances for BTC and USDT before order
    for balance in account_info['balances']:
        if balance['asset'] in ['BTC', 'USDT']:
            print(f"{balance['asset']}: {balance['free']}")

    symbol = "BTCUSDT"

    # Get the current market price for BTCUSDT
    ticker = client.get_ticker(symbol=symbol)
    current_price = float(ticker['lastPrice'])

    # Order price adjusted to an acceptable range
    order_price = current_price * 1.01

    # Get symbol details to understand the price filter
    symbol_info = client.get_symbol_info('BTCUSDT')
    price_filter = symbol_info['filters'][0]  # Assuming price filter is the first filter

    # Retrieve the allowed precision for the price
    price_precision = float(price_filter['tickSize'])

    # Adjust the order price to comply with the allowed precision
    order_price = round(order_price, int(-math.log10(price_precision)))

    # Order
    order = client.create_order(
        symbol='BTCUSDT',
        side=SIDE_BUY,
        type=ORDER_TYPE_LIMIT,
        timeInForce=TIME_IN_FORCE_GTC,
        quantity=0.001,
        price=str(order_price))

    print("Order placed successfully:")
    print(order)

    # List of trades made
    trades = client.get_my_trades(symbol=symbol)
    print("Our trades:")
    print(trades)

    # Print balances for BTC and USDT after order
    for balance in account_info['balances']:
        if balance['asset'] in ['BTC', 'USDT']:
            print(f"{balance['asset']}: {balance['free']}")

# Automatically run main when the script is executed
if __name__ == "__main__":
    main()
