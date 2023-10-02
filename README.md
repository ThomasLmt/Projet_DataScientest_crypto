# Set up VM

## Packages
pip install pandas<br>
pip install matplotlib<br>
pip install pymongo<br>
pip install python-dotenv<br>

## Copy the folder Bot_binance on the VM
- Drag & drop the folder using Visual Studio
- Or manage SSH key on your VM and put public key on GitHub or simply
git clone git@github.com:FGaloha/binance_bot.git

## Check .env contains necessary variables
touch .env
- Add the following variables<br>
MONGO_USER=userAdmin<br>
MONGO_PWD=userPassword<br>
POSTGRES_USER=useradmin<br>
POSTGRES_PASSWORD=userpassword<br>
- Generate from the terminal those used by airflow<br>
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env

## Be sure you have the rights
chmod +x setup.sh

## Run setup
./setup.sh

## Put the period in historical-data-engine.py
python3 -u historical-data-engine.py > engine.txt

# Use VM

- Access airflow
http://localhost:8080/

- Access jupyter notebook
http://localhost:8888/

*** *** *** *** *** ***

# Used for tests
- Uses a MongoDB market collection using historical-data-engine-FG.py and generating a machine learning model using service pyspark
- Models is copied on the bot_api at the end of the process and can be used by bot_apy.py

## To do only if the model does not already exist or need to be updated
- The COPY function does not work on the jupyter notebook image so it is managed after container creation<br>
docker cp ./pyspark/model-generation.py pyspark:/home/jovyan/model-generation.py

- generate model executing model-generation.py in pyspark container<br>
docker exec pyspark python /home/jovyan/model-generation.py

- copy model generated in pyspark container in botapi container<br>

  <b>VM</b><br>
docker cp pyspark:/home/jovyan/work/. jupyter_notebook_volume/<br>

  <b>MacOS</b><br>
docker cp pyspark:/home/jovyan/work/bot_api_model/ jupyter_notebook_volume/

## To do each time after docker-compose up -d
docker cp jupyter_notebook_volume/. botapi:/app/models/

# To test the api put localhost or the IP of your VM
- Check running<br>
http://localhost:8000
- Check prediction<br>
http://localhost:8000/prediction?eur_avg=30000.1&usdc_avg=30000.1&dai_avg=30000.1&gbp_avg=30000.1
