from fastapi import FastAPI, Header, Depends, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from typing import List, Union, Optional
from typing_extensions import Annotated
import base64
import time
import asyncio
from random import sample
from enum import Enum
from pydantic import BaseModel
import json
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.sql import SparkSession

# FastAPI creation
api = FastAPI(
    title='BotAPI',
    description="Easily predict btcusdt closing price",
    version="1.0"
)

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

model_path = "./models/bot_api_model"
loaded_model = LinearRegressionModel.load(model_path)

# Predict btcusdt closing price
def predict(data):
    #dense_vector = Vectors.dense(data)
    #row = Row(features=dense_vector)
    #new_data = spark.createDataFrame([row])
    #predictions = loaded_model.transform(new_data)
    # Extract the prediction values as a list of floats
    #prediction_values = predictions.select('prediction').rdd.map(lambda x: x[0]).collect()

    # Print the prediction values
    #predictions_list = [float(value) for value in prediction_values]  # Convert to floats
    #return predictions_list
    return data

@api.get('/')
def check_api():
    """
        To check if the API is properly working
    """
    return {
        'message': 'API ready to be used'
    }

@api.get('/prediction')
async def get_prediction(eur_avg: float, usdc_avg: float, dai_avg: float, gbp_avg: float):
    """
        To predict btcusdt closing price.
        There is 1 mandatory  parameter:
            - cryptos : list containing btceur, btcgpd, btcdai 7 days period average values
    """
    try:
        data = [eur_avg, usdc_avg, dai_avg, gbp_avg]
        prediction = predict(data)
        return JSONResponse(content=prediction)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
