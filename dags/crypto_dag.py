from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg
import warnings
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import time
#import numpy as np
import joblib
#import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 26),
    'retries': 1,
}

dag = DAG(
    'btc_market_analysis',
    default_args=default_args,
    description='A DAG for BTC market analysis',
    schedule_interval=None,
    catchup=False,
)

def load_data_from_mongo():
    """
    Loads data from a MongoDB collection and writes it to a Parquet file.

    This function initializes a Spark session, establishes a connection to a MongoDB database,
    fetches data from a specified collection, and saves this data into a Parquet file. It returns
    the path to the generated Parquet file.

    Parameters:
    - None

    Returns:
    - str: Path to the Parquet file where the data has been written.

    Notes:
    - The MongoDB connection parameters (e.g., host, port, username, etc.) are hard-coded within
      the function. Adjust these as needed based on your setup.
    - Ensure that the MongoDB Spark connector is installed and available to Spark.
    - The function overwrites the Parquet file if it already exists.

    Example:
    >>> parquet_file_path = load_data_from_mongo()
    >>> print(parquet_file_path)
    "/tmp/btc_data.parquet"
    """

    from pyspark.sql import SparkSession

    # MongoDB connection parameters
    mongo_host = "mongodb"
    mongo_port = 27017
    mongo_username = "userAdmin"
    mongo_password = "userPassword"
    auth_source = "admin"
    database_name = "binance_historical"
    collection_name = "markets"

    # Construct the MongoDB URI
    mongo_uri = (f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/{database_name}"
                f"?authSource={auth_source}")

    # Initialize a Spark session with MongoDB configurations
    spark = SparkSession.builder \
        .appName("BTCMarketAverages") \
        .config("spark.mongodb.input.uri", mongo_uri) \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    # Load data from the MongoDB collection into a Spark DataFrame
    df = spark.read.format('mongo') \
        .option('uri', mongo_uri) \
        .option('database', database_name) \
        .option('collection', collection_name) \
        .load()

    # Specify the path for the Parquet file
    parquet_path = "/tmp/btc_data.parquet"

    # Save the DataFrame to the Parquet file in overwrite mode
    df.write.mode('overwrite').parquet(parquet_path)

    return parquet_path

load_data = PythonOperator(
    task_id='load_data_from_mongo',
    python_callable=load_data_from_mongo,
    dag=dag,
)

def preprocess_data(**kwargs):
    """
    Preprocesses the cryptocurrency trading data to prepare for machine learning tasks.

    This function:
    1. Loads a Parquet file containing historical data from MongoDB.
    2. Uses Spark to compute a 7-day rolling average of the closing prices for selected markets.
    3. Filters and joins market-specific data columns.
    4. Converts the merged Spark DataFrame into a pandas DataFrame.

    Parameters:
    - **kwargs (dict): Keyword arguments containing the task instance to fetch Parquet file path from XCom in Airflow.

    Returns:
    - pd.DataFrame: A pandas DataFrame with the preprocessed data.

    Notes:
    - The 'SparkSession' should be appropriately initialized before calling this function.
    - The function assumes the 'df_btcusdt' DataFrame exists elsewhere in the application.

    """

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import col, avg

    # Fetching the Parquet file path from XCom
    ti = kwargs['ti']
    parquet_path = ti.xcom_pull(task_ids='load_data_from_mongo')

    # Read DataFrame from Parquet
    df = SparkSession.builder.getOrCreate().read.parquet(parquet_path)

    # Creating a window specification to calculate rolling averages
    window_spec = Window().partitionBy('market').orderBy('datetime').rowsBetween(-6, 0)

    # Filtering the dataframe for specific markets and computing the 7-day rolling average for 'close' column
    df_merge = df.filter((col('market') == 'BTCEUR') | (col('market') == 'BTCDAI') | (col('market') == 'BTCGBP') | (col('market') == 'BTCUSDC'))
    df_merge = df_merge.withColumn('rolling_avg_7d', avg('close').over(window_spec).cast('double'))

    # Renaming and selecting columns for merging
    df_merge = df_merge.select(df_merge.datetime.alias('datetime'), 'market', 'close_time', 'day', 'month', 'year', 'day_of_week', 'open', 'high', 'low', 'close', 'rolling_avg_7d', 'nbr_trades', 'volume')

    # Preparing the target dataframe (assuming df_btcusdt is defined somewhere else)
    df_btcusdt = df.filter(col('market') == 'BTCUSDT').select('datetime', 'close', 'day', 'month', 'year', 'day_of_week').withColumnRenamed('close', 'btcusdt_close')
    df_ml = df_btcusdt.select('datetime', 'btcusdt_close', 'day', 'month', 'year', 'day_of_week')

    # Merging the target dataframe with the feature dataframes
    for market in ['BTCEUR', 'BTCDAI', 'BTCGBP', 'BTCUSDC']:
        df_temp = df_merge.filter(col('market') == market).select('datetime', 'close', 'open', 'high', 'low', 'rolling_avg_7d', 'nbr_trades', 'volume')

        for col_name in ['close', 'open', 'high', 'low', 'rolling_avg_7d', 'nbr_trades', 'volume']:
            df_temp = df_temp.withColumnRenamed(col_name, market.lower() + '_' + col_name)
        df_ml = df_ml.join(df_temp, on='datetime', how='inner')

    df_pd = df_ml.toPandas()

    return df_pd

preprocess_data = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    provide_context=True,  # This is important to access task_instance and XCom
    dag=dag,
)

def machine_learning_training(**kwargs):
    """
    Trains a machine learning model on preprocessed cryptocurrency trading data.

    This function:
    1. Loads a preprocessed DataFrame from XCom.
    2. Separates the dataset into features and target variables.
    3. Splits the data into training and test sets.
    4. Defines a preprocessor that scales continuous features and one-hot encodes categorical features.
    5. Saves the preprocessor and datasets to files for subsequent tasks.

    Parameters:
    - **kwargs (dict): Keyword arguments containing the task instance to fetch data from XCom in Airflow.

    Returns:
    - None. The function saves datasets and preprocessor to files and pushes paths to XCom.

    Notes:
    - It's assumed that relevant libraries (e.g., joblib, train_test_split, OneHotEncoder, etc.) are imported elsewhere.
    - The 'ti' within kwargs refers to the task instance used within an Apache Airflow context.
    - Ensure the '/tmp/' directory is writable and has enough storage for storing intermediate files.

    """

    ti = kwargs['ti']

    # Retrieve the preprocessed df from XCom
    df_ml = ti.xcom_pull(task_ids='preprocess_data')

    # Features and target variable
    X = df_ml.drop(columns=['datetime', 'btcusdt_close'])
    y = df_ml['btcusdt_close']

    # Splitting data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    # Defining preprocessor
    continuous_features = [col for col in X.columns if 'open' in col or 'close' in col or 'high' in col or 'low' in col or 'rolling_avg_7d' in col or 'nbr_trades' in col or 'volume' in col]
    categorical_features = ['day', 'month', 'year', 'day_of_week']

    transformers = [
        ('onehot', OneHotEncoder(drop='first'), categorical_features),
        ('scaler', StandardScaler(), continuous_features)
    ]

    preprocessor = ColumnTransformer(transformers, remainder='passthrough')

    # Save the ColumnTransformer to a file
    joblib.dump(preprocessor, '/tmp/preprocessor.pkl')
    ti.xcom_push(key='preprocessor_path', value='/tmp/preprocessor.pkl')

    # Save datasets to files
    joblib.dump(X_train, '/tmp/X_train.pkl')
    ti.xcom_push(key='X_train_path', value='/tmp/X_train.pkl')

    joblib.dump(X_test, '/tmp/X_test.pkl')
    ti.xcom_push(key='X_test_path', value='/tmp/X_test.pkl')

    joblib.dump(y_train, '/tmp/y_train.pkl')
    ti.xcom_push(key='y_train_path', value='/tmp/y_train.pkl')

    joblib.dump(y_test, '/tmp/y_test.pkl')
    ti.xcom_push(key='y_test_path', value='/tmp/y_test.pkl')


machine_learning_training_task = PythonOperator(
    task_id='machine_learning_training',
    python_callable=machine_learning_training,
    provide_context=True,
    dag=dag
)

def train_model(model_name, **kwargs):
    """
    Train a specified machine learning model on preprocessed cryptocurrency trading data.

    This function:
    1. Fetches training and testing datasets along with the preprocessor from XCom.
    2. Selects the model to be trained based on the 'model_name' argument.
    3. Trains the selected model using a pipeline that includes preprocessing and the model itself.
    4. Predicts on the test data to compute the RMSE.
    5. Pushes the RMSE value to XCom for potential subsequent comparison or tasks.

    Parameters:
    - model_name (str): The name of the machine learning model to be trained. Expected values are "Linear Regression",
                       "Random Forest", "Decision Tree", and "XGBoost".
    - **kwargs (dict): Keyword arguments containing the task instance to fetch data from XCom in Airflow.

    Returns:
    - None. The function pushes RMSE to XCom for subsequent tasks.

    Notes:
    - It's assumed that relevant libraries (e.g., joblib, Pipeline, mean_squared_error, etc.) are imported elsewhere.
    - The 'ti' within kwargs refers to the task instance used within an Apache Airflow context.
    - Make sure to handle the case where an unsupported 'model_name' value is passed to avoid unintended behavior.

    """

    ti = kwargs['ti']

    # Retrieve paths from XCom
    X_train_path = ti.xcom_pull(task_ids='machine_learning_training', key='X_train_path')
    X_test_path = ti.xcom_pull(task_ids='machine_learning_training', key='X_test_path')
    y_train_path = ti.xcom_pull(task_ids='machine_learning_training', key='y_train_path')
    y_test_path = ti.xcom_pull(task_ids='machine_learning_training', key='y_test_path')
    preprocessor_path = ti.xcom_pull(task_ids='machine_learning_training', key='preprocessor_path')

    # Load datasets from file paths
    X_train = joblib.load(X_train_path)
    X_test = joblib.load(X_test_path)
    y_train = joblib.load(y_train_path)
    y_test = joblib.load(y_test_path)
    preprocessor = joblib.load(preprocessor_path)

    if model_name == "Linear_Regression":
        model = LinearRegression()
    elif model_name == "Random_Forest":
        model = RandomForestRegressor(n_estimators=100)
    elif model_name == "Decision_Tree":
        model = DecisionTreeRegressor()
    elif model_name == "XGBoost":
        model = XGBRegressor()

    pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])
    my_model = pipeline.fit(X_train, y_train)
    y_pred = my_model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred, squared=False)

    # Save RMSE to XCom for the best_model task to use
    ti.xcom_push(key=f'rmse_{model_name}', value=rmse)

    # Save the trained model
    model_save_path = f'/opt/airflow/dags/{model_name}_model.pkl'
    joblib.dump(my_model, model_save_path)
    print("Model created and saved at:", model_save_path)

linear_regression = PythonOperator(
    task_id='linear_regression',
    python_callable=train_model,
    op_args=["Linear_Regression"],
    provide_context=True,
    dag=dag
)

random_forest = PythonOperator(
    task_id='random_forest',
    python_callable=train_model,
    op_args=["Random_Forest"],
    provide_context=True,
    dag=dag
)

decision_tree = PythonOperator(
    task_id='decision_tree',
    python_callable=train_model,
    op_args=["Decision_Tree"],
    provide_context=True,
    dag=dag
)

xg_boost = PythonOperator(
    task_id='xg_boost',
    python_callable=train_model,
    op_args=["XGBoost"],
    provide_context=True,
    dag=dag
)

def best_model_selection(**kwargs):
    """
    Selects the best-performing model based on RMSE scores retrieved from XCom.

    This function:
    1. Retrieves the RMSE scores for various models (Linear Regression, Random Forest, Decision Tree, and XGBoost) from XCom.
    2. Determines the model with the lowest RMSE score.
    3. Prints the name of the best model and its associated RMSE score.

    Parameters:
    - **kwargs (dict): Keyword arguments containing the task instance to fetch data from XCom in Airflow.

    Returns:
    - None. The function prints the best model and its RMSE.

    Notes:
    - It's assumed that relevant libraries and functions are imported elsewhere.
    - The 'ti' within kwargs refers to the task instance used within an Apache Airflow context.
    - Task IDs used for 'xcom_pull' (e.g., 'linear_regression', 'random_forest') should correspond to the task IDs
      in the Airflow DAG where models are trained and their RMSE scores are stored in XCom.

    """

    ti = kwargs['ti']

    # Retrieve RMSE scores from XCom
    rmse_linear_regression = ti.xcom_pull(task_ids='linear_regression', key='rmse_Linear_Regression')
    rmse_random_forest = ti.xcom_pull(task_ids='random_forest', key='rmse_Random_Forest')
    rmse_decision_tree = ti.xcom_pull(task_ids='decision_tree', key='rmse_Decision_Tree')
    rmse_xg_boost = ti.xcom_pull(task_ids='xg_boost', key='rmse_XGBoost')

    # Store RMSE scores in a dictionary
    rmse_scores = {
        "Linear Regression": rmse_linear_regression,
        "Random Forest": rmse_random_forest,
        "Decision Tree": rmse_decision_tree,
        "XGBoost": rmse_xg_boost
    }

    # Determine the best model
    best_model_name = min(rmse_scores, key=rmse_scores.get)
    best_rmse_score = rmse_scores[best_model_name]

    # Print the best model and its RMSE score
    print(f"The best model is: {best_model_name}")
    print(f"Associated RMSE score: {best_rmse_score}")

best_model_task = PythonOperator(
    task_id='best_model',
    python_callable=best_model_selection,
    provide_context=True,
    dag=dag
)

# Set Task Dependencies
load_data >> preprocess_data
preprocess_data >> machine_learning_training_task
machine_learning_training_task >> [linear_regression, random_forest, decision_tree, xg_boost]
[linear_regression, random_forest, decision_tree, xg_boost] >> best_model_task
