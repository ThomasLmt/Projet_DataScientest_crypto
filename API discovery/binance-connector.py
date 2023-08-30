# Used to get production/live data from the Binance API
# Terminal command : python3 binance-connector.py > ../results/binance-connector.json

from binance.spot import Spot
import os
from dotenv import load_dotenv

load_dotenv()

# Access live Binance
client = Spot()

# Access testnet
#client = Spot(base_url='https://testnet.binance.vision')

# Get server timestamp
#print(client.time())

# Get klines of BTCUSDT at 1m interval
print(client.klines("ETHUSDT", "1m"))

# Get last 10 klines of BTCUSDT at 1h interval
#print(client.klines("BTCUSDT", "1h", limit=10))

# API key/secret are required for user data endpoints
# client testnet
#client = Spot(api_key=os.getenv('API_KEY_TESTNET'), api_secret=os.getenv('API_SECRET_TESTNET'))
# future testnet
#client = Spot(api_key=os.getenv('API_KEY_TESTNET_FUT'), api_secret=os.getenv('API_SECRET_TESTNET_FUT'))

# Vrai client Binance avec restriction IP: curl ifconfig.me
# client = Spot(api_key=os.getenv('API_KEY'), api_secret=os.getenv('API_SECRET'))

# Get account and balance information
# print(client.account())

# Post a new order
# params = {
#     'symbol': 'BTCUSDT',
#     'side': 'SELL',
#     'type': 'LIMIT',
#     'timeInForce': 'GTC',
#     'quantity': 0.002,
#     'price': 9500
# }

# response = client.new_order(**params)
#print(response)
