# Used to test orders using the Binance Testnet API
# Terminal command : python3 binance-connector.py > results/binance-connector.json

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import os
from dotenv import load_dotenv

load_dotenv()

#client = Client(api_key=os.getenv('API_KEY_TESTNET'), api_secret=os.getenv('API_SECRET_TESTNET'), testnet=True)
# print(client.get_account())

client = Client(api_key=os.getenv('API_KEY'), api_secret=os.getenv('API_SECRET'), testnet=True)

symbol = "BTCEUR"
interval = Client.KLINE_INTERVAL_1HOUR  # You can change this interval as needed

# klines = client.get_historical_klines(symbol, interval, "1 Jan, 2021", "20 Aug, 2023")
# print(klines)

symbol = "EURBTC"

trades = client.get_my_trades(symbol=symbol)
print(trades)
