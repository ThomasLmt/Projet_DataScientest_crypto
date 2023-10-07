# Code to test the API
# Terminal command : python3 bot_api/test.py >> bot_api/test_api.txt

import requests

# Define the API address and port
api_address = '54.75.106.132'
api_port = 8000

# Request
params = {
    #'datetime': 1597125600000,
    'day': 11,
    'month': 8,
    'year': 2020,
    'day_of_week': 2,
    'btceur_close': 10080.11,
    'btceur_open': 10152.47,
    'btceur_high': 10152.47,
    'btceur_low': 10050.00,
    'btceur_rolling_avg_7d': 10140.278571,
    'btceur_nbr_trades': 492.0,
    'btceur_volume': 10.230015,
    'btcdai_close': 10030.03,
    'btcdai_open': 11824.55,
    'btcdai_high': 13999.00,
    'btcdai_low': 3001.00,
    'btcdai_rolling_avg_7d': 10030.030000,
    'btcdai_nbr_trades': 14.0,
    'btcdai_volume': 0.763433,
    'btcgbp_close': 9048.33,
    'btcgbp_open': 9113.94,
    'btcgbp_high': 9128.31,
    'btcgbp_low': 9040.08,
    'btcgbp_rolling_avg_7d': 9110.301429,
    'btcgbp_nbr_trades': 155.0,
    'btcgbp_volume': 2.545390,
    'btcusdc_close': 11753.98,
    'btcusdc_open': 11856.34,
    'btcusdc_high': 11857.96,
    'btcusdc_low': 11712.37,
    'btcusdc_rolling_avg_7d': 11851.060000,
    'btcusdc_nbr_trades': 1297.0,
    'btcusdc_volume': 243.836750
}

r = requests.get(f'http://{api_address}:{api_port}')
print(r.status_code)
print(r.json())

r = requests.get(f'http://{api_address}:{api_port}/prediction', params=params)
print(r.status_code)
print(r.json())
