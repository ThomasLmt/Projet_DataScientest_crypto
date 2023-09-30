### Set up VM
pip install pandas
pip install matplotlib
pip install pymongo
pip install python-dotenv

# Manage SSH key on your VM and put public key on GitHub
git clone git@github.com:FGaloha/binance_bot.git

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

# Put the period in historical-data-engine.py
python3 -u historical-data-engine.py > engine.txt

### Access airflow
http://localhost:8080/

### Access jupyter notebook
http://localhost:8888/

### Used for tests
# Uses a MongoDB market collection using historical-data-engine-FG.py and generating a machine learning model using service pyspark
# Models is copied on the bot_api at the end of the process and can be used by bot_apy.py

# only if model does not exist
# The COPY function does not work on the jupyter notebook image so it is managed after container creation
docker cp ./pyspark/model-generation.py pyspark:/home/jovyan/model-generation.py

# only if model does not exist
# generate model executing model-generation.py in pyspark container
docker exec pyspark python /home/jovyan/model-generation.py

# only if model does not exist
# copy model generated in pyspark container in botapi container
docker cp pyspark:/home/jovyan/work/bot_api_model/ jupyter_notebook_volume/

# To do each time after docker-compose up -d
docker cp jupyter_notebook_volume/. botapi:/app/models/

### Test api put localhost or the IP of your VM
# Running
http://localhost:8000
# Prediction
http://localhost:8000/prediction?eur_avg=30000.1&usdc_avg=30000.1&dai_avg=30000.1&gbp_avg=30000.1