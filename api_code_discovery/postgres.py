# Codes used to test postgres data and manage grants
# Terminal command : python3 -u postgres.py >> results/postgres.txt
import psycopg2
import os
# db_params = {
#         'database': 'binance_stream',
#         'user': os.getenv('POSTGRES_USER'),
#         'password': os.getenv('POSTGRES_PASSWORD'),
#         'host': 'localhost',
#         'port': '5432'
#     }

# conn = psycopg2.connect(**db_params)
# cursor = conn.cursor()

# query = "SELECT event_timestamp,event_date,open,high ,low ,close ,volume ,start_timestamp ,\
#         close_timestamp ,qav ,nbr_trades ,taker_base_vol ,taker_quote_vol , symbol  FROM market LIMIT 5;"

# cursor.execute(query)

# # Fetch the results
# records = cursor.fetchall()

# # Print the records
# for record in records:
#     print(record)

# cursor.close()
# conn.close()


# table average 7d creation
# db_params = {
#     'database': 'binance_stream',
#     'user': os.getenv('POSTGRES_USER'),
#     'password': os.getenv('POSTGRES_PASSWORD'),
#     'host': 'localhost',
#     'port': '5432'
# }


# # Connexion to the new database

# conn = psycopg2.connect(**db_params)
# conn.autocommit = True
# cursor = conn.cursor()

# create_markets_7d_data_table = """
# CREATE TABLE markets_7d_data (
#     id SERIAL PRIMARY KEY,
#     start_timestamp BIGINT,
#     day INTEGER,
#     month INTEGER,
#     year INTEGER,
#     day_of_week INTEGER,
#     btceur_close DECIMAL,
#     btceur_open DECIMAL,
#     btceur_high DECIMAL,
#     btceur_low DECIMAL,
#     btceur_rolling_avg_7d DECIMAL,
#     btceur_nbr_trades INTEGER,
#     btceur_volume DECIMAL,
#     btcdai_close DECIMAL,
#     btcdai_open DECIMAL,
#     btcdai_high DECIMAL,
#     btcdai_low DECIMAL,
#     btcdai_rolling_avg_7d DECIMAL,
#     btcdai_nbr_trades INTEGER,
#     btcdai_volume DECIMAL,
#     btcgbp_close DECIMAL,
#     btcgbp_open DECIMAL,
#     btcgbp_high DECIMAL,
#     btcgbp_low DECIMAL,
#     btcgbp_rolling_avg_7d DECIMAL,
#     btcgbp_nbr_trades INTEGER,
#     btcgbp_volume DECIMAL,
#     btcusdc_close DECIMAL,
#     btcusdc_open DECIMAL,
#     btcusdc_high DECIMAL,
#     btcusdc_low DECIMAL,
#     btcusdc_rolling_avg_7d DECIMAL,
#     btcusdc_nbr_trades INTEGER,
#     btcusdc_volume DECIMAL

# );
# """
# cursor.execute(create_markets_7d_data_table)

# print("Table markets_7d_data has been created successfully")

# # Fermeture de la connexion
# conn.close()

### Grant rights on markets_7d_data
# conn_params = {
#         'database': 'binance_stream',
#         'user': os.getenv('POSTGRES_USER'),
#         'password': os.getenv('POSTGRES_PASSWORD'),
#         'host': 'localhost',
#         'port': '5432'
#     }

# # The SQL statement to grant permissions
# grant_statement = "GRANT SELECT, INSERT, UPDATE ON markets_7d_data TO useradmin;"

# try:
#     # Connect to the PostgreSQL database
#     conn = psycopg2.connect(**conn_params)
#     cursor = conn.cursor()

#     # Execute the GRANT statement
#     cursor.execute(grant_statement)

#     # Commit the transaction
#     conn.commit()
#     print("Permissions granted successfully.")

# except psycopg2.Error as e:
#     print("Error: Unable to grant permissions -", e)

# finally:
#     # Close the cursor and connection
#     cursor.close()
#     conn.close()

### Test content
db_params = {
        'database': 'binance_stream',
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5432'
    }

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

#query = "SELECT * FROM markets_7d_data LIMIT 5;"
query = "SELECT * FROM markets_7d_data;"

cursor.execute(query)

# Fetch the results
records = cursor.fetchall()

# Print the records
for record in records:
    print(record)

cursor.close()
conn.close()

### Grant rights on markets_7d_data_id_seq
# conn_params = {
#         'database': 'binance_stream',
#         'user': os.getenv('POSTGRES_USER'),
#         'password': os.getenv('POSTGRES_PASSWORD'),
#         'host': 'localhost',
#         'port': '5432'
#     }

# # # The SQL statement to grant permissions
# grant_statement = "GRANT USAGE, SELECT ON SEQUENCE markets_7d_data_id_seq TO useradmin;"

# try:
#     # Connect to the PostgreSQL database
#     conn = psycopg2.connect(**conn_params)
#     cursor = conn.cursor()

#     # Execute the GRANT statement
#     cursor.execute(grant_statement)

#     # Commit the transaction
#     conn.commit()
#     print("Permissions granted successfully.")

# except psycopg2.Error as e:
#     print("Error: Unable to grant permissions -", e)

# finally:
#     # Close the cursor and connection
#     cursor.close()
#     conn.close()

### Grant rights update on markets_7d_data_id_seq
# conn_params = {
#         'database': 'binance_stream',
#         'user': os.getenv('POSTGRES_USER'),
#         'password': os.getenv('POSTGRES_PASSWORD'),
#         'host': 'localhost',
#         'port': '5432'
#     }

# # # The SQL statement to grant permissions
# grant_statement = "GRANT UPDATE ON SEQUENCE markets_7d_data_id_seq TO useradmin;"

# try:
#     # Connect to the PostgreSQL database
#     conn = psycopg2.connect(**conn_params)
#     cursor = conn.cursor()

#     # Execute the GRANT statement
#     cursor.execute(grant_statement)

#     # Commit the transaction
#     conn.commit()
#     print("Permissions granted successfully.")

# except psycopg2.Error as e:
#     print("Error: Unable to grant permissions -", e)

# finally:
#     # Close the cursor and connection
#     cursor.close()
#     conn.close()
