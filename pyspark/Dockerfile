FROM jupyter/pyspark-notebook

USER root

# Install pymongo and pyspark and mongo-spark-connector
RUN pip install --no-cache-dir pyspark pymongo
# RUN pip install --no-cache-dir mongo-spark-connector
RUN wget -O /usr/local/spark/jars/mongo-spark-connector_2.12-3.0.2.jar \
  https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.2/mongo-spark-connector_2.12-3.0.2.jar

# Download and add the MongoDB Java Driver JAR
RUN wget -O /usr/local/spark/jars/mongo-java-driver-3.12.13.jar \
  https://search.maven.org/remotecontent?filepath=org/mongodb/mongo-java-driver/3.12.13/mongo-java-driver-3.12.13.jar

# ENV PYSPARK_SUBMIT_ARGS="--jars /usr/local/spark/jars/mongo-spark-connector_2.12-3.0.2.jar,/usr/local/spark/jars/mongo-java-driver-3.12.13.jar pyspark-shell"

# To run jupyter without password
RUN echo "c.NotebookApp.token = ''" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

USER $NB_UID
