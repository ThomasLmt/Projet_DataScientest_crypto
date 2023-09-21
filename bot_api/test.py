import requests

# Define the API address and port
api_address = 'botapi'
api_port = 8000

# Request
params = {
    'eur_avg': 3.1,
    'usdc_avg': 1.2,
    'dai_avg': 8.5,
    'gbp_avg': 2.5
}

r = requests.get(f'http://{api_address}:{api_port}/prediction', params=params)
print(r.status_code)
print(r.json())
