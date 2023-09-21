# docker-compose launched
docker-compose down

# docker-compose launched
docker-compose up --build -d

# The COPY function does not work on the jupyter notebook image so it is managed after container creation
docker cp ./pyspark/model-generation.py pyspark:/home/jovyan/model-generation.py

# generate model executing model-generation.py in pyspark container
docker exec pyspark python /home/jovyan/model-generation.py

# copy model generated in pyspark container in botapi container
# docker cp pyspark:/home/jovyan/work/bot_api_model/ jupyter_notebook_volume/
docker cp jupyter_notebook_volume/ botapi:/app/models/

#sleep 10

# Start the API using uvicorn in the botapi container
#docker exec botapi uvicorn bot_api:api --host 0.0.0.0 --port 8000
