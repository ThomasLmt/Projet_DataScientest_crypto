# Used to test orders using the Binance Testnet API
# Terminal command : python3 python-binance-connector.py > ../results/python-binance-connector.json

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from binance.enums import *
import math
import os
from dotenv import load_dotenv

load_dotenv()

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
