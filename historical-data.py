#  Terminal command : python3 historical-data.py > results/historical-data.json
import requests
import json
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

# Binance API used to get historical data
url = 'https://api.binance.com/api/v3/klines'

# Param to get chosen data
start_period = str(int(dt.datetime(2017,1,1).timestamp()*1000)) # start date period
end_period = str(int(dt.datetime(2023,8,1).timestamp()*1000)) # end date period
symbol = 'BTCEUR' # crypto market
interval = '1w' # interval between records 1M = 1 month

par = {'symbol': symbol, 'interval': interval, 'startTime': start_period, 'endTime': end_period}

# API Request
data = pd.DataFrame(json.loads(requests.get(url, params=par).text))

# DataFrame tuning
data.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume','close_time', 'qav', 'nbr_trades','taker_base_vol', 'taker_quote_vol', 'unused_field']
data.index = [dt.datetime.fromtimestamp(d/1000.0) for d in data.datetime]
data.sort_index()
data=data.astype(float)

# DataFrame head
print(data.head())

# Chart
data["close"].plot(title = 'BTCEUR', legend = 'close')
plt.show()
