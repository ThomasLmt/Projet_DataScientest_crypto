from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, coalesce

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

df.select('market').distinct().collect()

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

# Show the final DataFrame
df_merged.show(5)

df_btcusdt.show(5)

df_btcusdt.count()

df_eur.count()

df_usdc.count()

df_dai.count()

df_gbp.count()

df_merged.describe().toPandas()

from datetime import datetime
# Assuming df_merged is the DataFrame
first_close_time = df_merged.select('usdt_close_time').first()[0]
# Convert the Unix timestamp to a readable date format
readable_date = datetime.fromtimestamp(first_close_time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
print("First close_time usdt in readable date format:", readable_date)

first_close_time = df_merged.select('usdc_datetime').first()[0]
# Convert the Unix timestamp to a readable date format
readable_date = datetime.fromtimestamp(first_close_time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
print("First usdc timestamp in readable date format:", readable_date)

first_close_time = df_merged.select('eur_datetime').first()[0]
# Convert the Unix timestamp to a readable date format
readable_date = datetime.fromtimestamp(first_close_time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
print("First eur timestamp in readable date format:", readable_date)

df_merged = df_merged.drop('usdc_datetime', 'date', 'usdt_close_time', 'eur_datetime')

df_merged.limit(5).collect()

from pyspark.ml.linalg import DenseVector

rdd_ml = df_merged.rdd.map(lambda x: (x[-1], DenseVector(x[0:-1])))

df_ml = spark.createDataFrame(rdd_ml, ['label', 'features'])

df_ml.show(10)

df_ml.limit(5).collect()

train, test = df_ml.randomSplit([.8, .2], seed= 42)

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol='label', featuresCol= 'features')
linearModel = lr.fit(train)
predicted = linearModel.transform(test)
predicted.limit(5).collect()
print("RMSE:", linearModel.summary.rootMeanSquaredError)


from pyspark.ml.linalg import DenseVector
rdd_ml = df_merged.rdd.map(lambda x: (x[-1], DenseVector(x[0:-1])))
df_ml = spark.createDataFrame(rdd_ml, ['label', 'features'])
print(df_ml.limit(5).collect())
train, test = df_ml.randomSplit([.8, .2], seed= 42)
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol='label', featuresCol= 'features')
linearModel = lr.fit(train)
predicted = linearModel.transform(test)
print(predicted.limit(5).collect())
print("RMSE:", linearModel.summary.rootMeanSquaredError)
print("R-squared:", linearModel.summary.r2)

# Save the model using MLlib persistence
model_path = "./work/my_linear_model"
linearModel.save(model_path)

from pyspark.ml.regression import LinearRegressionModel
# Load the model
loaded_model = LinearRegressionModel.load(model_path)

from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

# Data format to send to the API
data = [10145.245, 11856.9675, 10030.03, 9114.8912]

# What the API will do with it
dense_vector = Vectors.dense(data)
row = Row(features=dense_vector)
new_data = spark.createDataFrame([row])
#new_data.show()

predictions = loaded_model.transform(new_data)

predictions.show()

predictions.select('prediction').show()

# Extract the prediction values as a list of floats
prediction_values = predictions.select('prediction').rdd.map(lambda x: x[0]).collect()

# Print the prediction values
for value in prediction_values:
    # What the API will return
    print(value)

predicted.count()

import matplotlib.pyplot as plt

# Assuming predicted is the DataFrame
labels = predicted.select('label').collect()
predictions = predicted.select('prediction').collect()

# Extract the values from the collected rows
labels = [float(row.label) for row in labels]
predictions = [float(row.prediction) for row in predictions]

# Create a plot
plt.figure(figsize=(10, 6))
plt.plot(labels, label='Labels')
plt.plot(predictions, label='Predictions')
plt.xlabel('Data Point')
plt.ylabel('Value')
plt.legend()
plt.show()

from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_ml)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_ml.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

treeModel = model.stages[1]
# summary only
print(treeModel)

from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_ml)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_ml.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

treeModel = model.stages[1]
# summary only
print(treeModel)



from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_ml)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_ml.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestRegressor(featuresCol="indexedFeatures")

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, rf])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

rfModel = model.stages[1]
print(rfModel)  # summary only




from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_ml)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_ml.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestRegressor(featuresCol="indexedFeatures")

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, rf])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

rfModel = model.stages[1]
print(rfModel)  # summary only

spark.stop()
