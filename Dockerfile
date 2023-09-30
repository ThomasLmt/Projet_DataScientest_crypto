# Start with the Apache Airflow image as the base
FROM apache/airflow:2.7.1

# Switch to root to change permissions and install software
USER root

# Grant permissions to resolve potential issues with apt-get update
RUN chmod -R 777 /var/lib/apt/lists/

# Install the necessary dependencies
RUN apt-get update && apt-get install -y software-properties-common openjdk-11-jdk
#wget

# Create the necessary directories
# RUN mkdir -p /home/airflow/spark/jars/ && chown -R airflow:airflow /home/airflow/spark

# Switch back to the default Airflow user for safety
USER airflow

RUN pip install --no-cache-dir pyspark pymongo scikit-learn xgboost
# RUN pip install pyspark==3.4.1 scikit-learn xgboost

# RUN wget -O /home/airflow/spark/jars/mongo-spark-connector_2.12-3.0.2.jar \
#   https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.2/mongo-spark-connector_2.12-3.0.2.jar

# # Download and add the MongoDB Java Driver JAR
# RUN wget -O /home/airflow/spark/jars/mongo-java-driver-3.12.13.jar \
#   https://search.maven.org/remotecontent?filepath=org/mongodb/mongo-java-driver/3.12.13/mongo-java-driver-3.12.13.jar
