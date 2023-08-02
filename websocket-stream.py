# Terminal command : python3 websocket-stream.py > results/websocket-stream.json

import time
import logging

logging.basicConfig(level=logging.INFO)

# WebSocket Stream Client
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient

def message_handler(_, message):
    logging.info(message)

my_client = SpotWebsocketStreamClient(on_message=message_handler)

# Subscribe to a single symbol stream
#my_client.agg_trade(symbol="btcusdt")
my_client.kline(symbol="btcusdt",interval='1s')
time.sleep(15)
logging.info("closing ws connection")
my_client.stop()
