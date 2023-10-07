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
from pyspark.sql import Row
from pyspark.sql import SparkSession
import joblib
import pandas as pd

# FastAPI creation
api = FastAPI(
    title='BotAPI',
    description="Easily predict btcusdt closing price",
    version="1.0"
)

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

model_path = "./Random_Forest_model.pkl"
loaded_model = joblib.load(model_path)

# Predict btcusdt closing price
def predict(data):
    # Create a DataFrame with named columns
    column_names = ['day', 'month', 'year', 'day_of_week', 'btceur_close', 'btceur_open', 'btceur_high', 'btceur_low', 'btceur_rolling_avg_7d',
                    'btceur_nbr_trades', 'btceur_volume', 'btcdai_close', 'btcdai_open', 'btcdai_high', 'btcdai_low', 'btcdai_rolling_avg_7d',
                    'btcdai_nbr_trades', 'btcdai_volume', 'btcgbp_close', 'btcgbp_open', 'btcgbp_high', 'btcgbp_low', 'btcgbp_rolling_avg_7d',
                    'btcgbp_nbr_trades', 'btcgbp_volume', 'btcusdc_close', 'btcusdc_open', 'btcusdc_high', 'btcusdc_low', 'btcusdc_rolling_avg_7d',
                    'btcusdc_nbr_trades', 'btcusdc_volume']

    data_df = pd.DataFrame([data], columns=column_names)

    # Predict using the loaded model
    predictions = loaded_model.predict(data_df)

    # Return the first prediction as a float
    return float(predictions[0])

@api.get('/')
def check_api():
    """
        To check if the API is properly working
    """
    return {
        'message': 'API ready to be used'
    }

@api.get('/prediction')
async def get_prediction(day: int,month: int,year: int,day_of_week: int,btceur_close: float,btceur_open: float,btceur_high: float,btceur_low: float,btceur_rolling_avg_7d: float,btceur_nbr_trades: float,btceur_volume: float,btcdai_close: float,btcdai_open:float,btcdai_high: float,btcdai_low: float,btcdai_rolling_avg_7d: float,btcdai_nbr_trades: float,btcdai_volume: float,btcgbp_close: float,btcgbp_open: float,btcgbp_high: float,btcgbp_low: float,btcgbp_rolling_avg_7d: float,btcgbp_nbr_trades: float,btcgbp_volume: float,btcusdc_close: float,btcusdc_open: float,btcusdc_high: float,btcusdc_low: float,btcusdc_rolling_avg_7d: float,btcusdc_nbr_trades: float,btcusdc_volume: float):
    """
        To predict btcusdt closing price.
        There is 32 mandatory  parameters:
            - cryptos : list containing day, month, year, 7 days period average values of 4 cryptos...
    """
    try:
        data = [day,month,year,day_of_week,btceur_close,btceur_open,btceur_high,btceur_low,btceur_rolling_avg_7d,btceur_nbr_trades,btceur_volume,btcdai_close,btcdai_open,btcdai_high,btcdai_low,btcdai_rolling_avg_7d,btcdai_nbr_trades,btcdai_volume,btcgbp_close,btcgbp_open,btcgbp_high,btcgbp_low,btcgbp_rolling_avg_7d,btcgbp_nbr_trades,btcgbp_volume,btcusdc_close,btcusdc_open,btcusdc_high,btcusdc_low,btcusdc_rolling_avg_7d,btcusdc_nbr_trades,btcusdc_volume]
        prediction = predict(data)
        return JSONResponse(content=prediction)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
