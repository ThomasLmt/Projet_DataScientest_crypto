from binance.spot import Spot

client = Spot()

# Access testnet
client = Spot(base_url='https://testnet.binance.vision')
#print(client.base_url)

# Get server timestamp
print(client.time())

# Get klines of BTCUSDT at 1m interval
#print(client.klines("BTCUSDT", "1m"))

# Get last 10 klines of BNBUSDT at 1h interval
#print(client.klines("BNBUSDT", "1h", limit=10))

# API key/secret are required for user data endpoints

# client testnet
client = Spot(api_key='FhVC53YuOBMmfuEUBwqVZ2WR8aurM2BYfX4gru5fWfROXJDA2Qj4fXQKfshaBXkd', api_secret='w52djXRnEh7NM68goEnR3oeyUocr3kiXV4BGSCqabdI6EX4ml1g6ql4Oij8CoQ1Q')

# Vrai client Binance avec restriction IP: curl ifconfig.me
# client = Spot(api_key='Bu1wtsxgYsHXnUYma9alLgeaZjDPYSUmwkGc6CvlUfau0BWfPosqhsNCfnXFTyPA', api_secret='nApQjyvUzyhBQRsZgD6AWy8ezoh36ousvfak14pWCiA9M95gcGNcovFlpQuTHOyq')

# Get account and balance information
print(client.get_asset_balance())

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
