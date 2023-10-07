# Start with the Apache Airflow image as the base
FROM apache/airflow:2.7.1

# Switch to root to change permissions and install software
USER root

# Grant permissions to resolve potential issues with apt-get update
RUN chmod -R 777 /var/lib/apt/lists/

# Install the necessary dependencies
RUN apt-get update && apt-get install -y software-properties-common openjdk-11-jdk

# Switch back to the default Airflow user for safety
USER airflow

RUN pip install --no-cache-dir pyspark pymongo scikit-learn xgboost
