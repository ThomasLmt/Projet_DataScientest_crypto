### Set up VM
pip install pandas
pip install matplotlib
pip install pymongo
pip install python-dotenv

# Go on the folder of the app and download data from Binance API
cd binance_bot

# Put the period in historical-data-engine.py
python3 -u historical-data-engine.py > engine.txt

# Check .env contains necessary variables
touch .env
# Add the following variables
MONGO_USER=userAdmin
MONGO_PWD=userPassword
POSTGRES_USER=useradmin
POSTGRES_PASSWORD=userpassword
# Generate from the terminal those used by airflow
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env

# Be sure you have the rights
chmod +x setup.sh

# Run setup
./setup.sh

### Test api put localhost or the IP of your VM
# Running
http://localhost:8000
# Prediction
http://localhost:8000/prediction?eur_avg=30000.1&usdc_avg=30000.1&dai_avg=30000.1&gbp_avg=30000.1

### Access jupyter notebook
http://localhost:8888/

### Access airflow
http://localhost:8080/
