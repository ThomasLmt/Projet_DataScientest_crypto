import requests
import json
from datetime import datetime

base_url = "https://data-api.binance.vision/api/v3"
endpoint = "/avgPrice"

symbol = "BTCEUR"

def get_price(symbol):
    url = f"{base_url}{endpoint}?symbol={symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Prettify the JSON output
        #formatted_data = json.dumps(data, indent=2)
        print({"Currency": symbol,"date": datetime.now().strftime('%Y-%m-%d'),"time": datetime.now().strftime('%H:%M:%S'),"price": data["price"]})
    else:
        print(f"Error: {response.status_code} - {response.text}")

get_price("BTCEUR")
get_price("ETHEUR")
