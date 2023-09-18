from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a PySpark session
spark = SparkSession.builder.appName("MongoDB historical data to PySpark").getOrCreate()
print(spark.version)

# Read data from MongoDB into a PySpark DataFrame
mongodb_uri = "mongodb://userAdmin:userPassword@mongodb:27017/binance_historical.markets"
data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongodb_uri).load()

# Perform PySpark operations on df
df = data.filter(col("market") == "ETHUSDC") \
    .select("timestamp_date", "close")

# Stop the PySpark session when done
spark.stop()
