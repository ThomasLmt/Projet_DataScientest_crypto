from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, coalesce
import warnings
warnings.filterwarnings('ignore')

# Create a Spark session
spark = SparkSession.builder \
    .appName("BTCMarketAverages") \
    .getOrCreate()

# Load data from MongoDB collection into PySpark DataFrame
df = spark.read.format('mongo') \
    .option('uri', 'mongodb://userAdmin:userPassword@mongo_binance_historical') \
    .option('database','binance_historical') \
    .option('collection','markets') \
    .load()

# Calculate 7-day rolling averages for BTCEUR and BTCUSDC
window_spec = Window().partitionBy('market').orderBy('datetime').rowsBetween(-7, 0)

# feature btceur
df_eur = df.filter(col('market') == 'BTCEUR').select('date','close_time', 'close', 'market', 'datetime')
df_eur = df_eur.withColumn('btceur_average', avg('close').over(window_spec).cast('double'))
# Select relevant columns and alias the DataFrames
df_eur = df_eur.select(df_eur.datetime.alias('eur_datetime'), 'close_time', 'btceur_average')

# feature btcdai
df_dai = df.filter(col('market') == 'BTCDAI').select('date','close_time', 'close', 'market', 'datetime')
df_dai = df_dai.withColumn('btcdai_average', avg('close').over(window_spec).cast('double'))
df_dai = df_dai.select(df_dai.datetime.alias('dai_datetime'), 'close_time', 'btcdai_average')

# feature btcgbp
df_gbp = df.filter(col('market') == 'BTCGBP').select('date','close_time', 'close', 'market', 'datetime')
df_gbp = df_gbp.withColumn('btcgbp_average', avg('close').over(window_spec).cast('double'))
df_gbp = df_gbp.select(df_gbp.datetime.alias('gbp_datetime'), 'close_time', 'btcgbp_average')

# feature btcusdc
df_usdc = df.filter(col('market') == 'BTCUSDC').select('date','close_time', 'close', 'market', 'datetime')
df_usdc = df_usdc.withColumn('btcusdc_average', avg('close').over(window_spec).cast('double'))
df_usdc = df_usdc.select(df_usdc.datetime.alias('usdc_datetime'), 'close_time', 'btcusdc_average')

# target
df_btcusdt = df.filter(col('market') == 'BTCUSDT').select('date','close_time', 'close')

# Merge starting from df_eur, then join df_btcusdt and df_usdc based on close_time
df_merged = df_eur.join(df_btcusdt, 'close_time', 'inner') \
                  .join(df_usdc, 'close_time', 'inner') \
                  .join(df_dai, 'close_time', 'inner') \
                  .join(df_gbp, 'close_time', 'inner') \
                  .select('eur_datetime', 'usdc_datetime', 'btceur_average', 'btcusdc_average', 'btcdai_average', 'btcgbp_average', 'close_time', 'date', 'close') \
                  .distinct().orderBy('close_time')

# title clarification
df_merged = df_merged.select('eur_datetime', 'usdc_datetime', 'btceur_average', 'btcusdc_average', 'btcdai_average', 'btcgbp_average', df_merged.close_time.alias('usdt_close_time'), 'date', df_merged.close.alias('btcusdt_close_price'))

df_merged = df_merged.drop('usdc_datetime', 'date', 'usdt_close_time', 'eur_datetime')

from pyspark.ml.linalg import DenseVector
rdd_ml = df_merged.rdd.map(lambda x: (x[-1], DenseVector(x[0:-1])))
df_ml = spark.createDataFrame(rdd_ml, ['label', 'features'])

train, test = df_ml.randomSplit([.8, .2], seed= 42)
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol='label', featuresCol= 'features')
linearModel = lr.fit(train)
predicted = linearModel.transform(test)

# Save the model using MLlib persistence
model_path = "./work/bot_api_model"
# If the model does not exist
# linearModel.save(model_path)
# If it already exists
linearModel.write().overwrite().save(model_path)

# Stop the PySpark session when done
spark.stop()
