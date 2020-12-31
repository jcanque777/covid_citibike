import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def read_csv(weather_csv, weather_output_csv):
    # load data
    df = pd.read_csv(weather_csv)

    # only use 1 source
    df = df[df["SOURCE"]==7]

    # keep only relevant
    cols_keep_list = ['DATE', 'REPORT_TYPE', "HourlyDryBulbTemperature", "HourlyRelativeHumidity", "HourlyPrecipitation", "HourlyWindSpeed"]
    # create new dataframe
    df = df[cols_keep_list]

    # create empty timestamp column and get timestamp from DATE column
    df["ts"] = 0
    df["ts"] = pd.to_datetime(df.DATE)
    
    # create hour and data colum
    df["date"]= df["ts"].dt.date
    df["hour"]= df["ts"].dt.hour
    
    # only keep rows with data in hourly dry bulb temp
    df = df[~df["HourlyDryBulbTemperature"].isna()]
    # sort by timestamp
    df.sort_values(by="ts", inplace=True)
    # reset index
    df.reset_index(inplace=True)
    # use previous correct value to fillna
    df.fillna(method="ffill", inplace=True)
    df = df[['date', 'hour', 'REPORT_TYPE', 'HourlyDryBulbTemperature',
    'HourlyRelativeHumidity', 'HourlyPrecipitation', 'HourlyWindSpeed']]
    df.to_csv(weather_output_csv)
    print(f"Dataframe Shape: {df.shape}")
    print(f"Dataframe Preview: {df.head(5)}")




default_args = {
    'owner': 'johnrick',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('load_data_dag',
          default_args=default_args,
          description='load data to pyspark'#,
        #   schedule_interval='0 * * * *'
        )

transform_weather = PythonOperator(
    task_id="transform_weather",
    python_callable=read_csv,
    op_kwargs={
        "weather_csv": "data/nyc_daily_weather.csv",
        "weather_output_csv": "data/transformed_nyc_daily_weather.csv"
    },
    dag=dag
)

read_weather