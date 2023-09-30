# Cleaning
docker-compose down

# Launch
docker-compose up --build -d

### Used for tests
# Uses a MongoDB market collection using historical-data-engine-FG.py and generating a machine learning model using service pyspark
# Models is copied on the bot_api at the end of the process and can be used by bot_apy.py

# The COPY function does not work on the jupyter notebook image so it is managed after container creation
docker cp ./pyspark/model-generation.py pyspark:/home/jovyan/model-generation.py

# generate model executing model-generation.py in pyspark container
docker exec pyspark python /home/jovyan/model-generation.py

# copy model generated in pyspark container in botapi container
docker cp jupyter_notebook_volume/ botapi:/app/models/
