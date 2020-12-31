import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests


# def extract_and_transform_weather(weather_csv, weather_output_csv):
#     # load data
#     df = pd.read_csv(weather_csv)

#     # only use 1 source
#     df = df[df["SOURCE"]==7]

#     # keep only relevant
#     cols_keep_list = ['DATE', 'REPORT_TYPE', "HourlyDryBulbTemperature", "HourlyRelativeHumidity", "HourlyPrecipitation", "HourlyWindSpeed"]
#     # create new dataframe
#     df = df[cols_keep_list]

#     # create empty timestamp column and get timestamp from DATE column
#     df["ts"] = 0
#     df["ts"] = pd.to_datetime(df.DATE)
    
#     # create hour and data colum
#     df["date"]= df["ts"].dt.date
#     df["hour"]= df["ts"].dt.hour
    
#     # only keep rows with data in hourly dry bulb temp
#     df = df[~df["HourlyDryBulbTemperature"].isna()]
#     # sort by timestamp
#     df.sort_values(by="ts", inplace=True)
#     # reset index
#     df.reset_index(inplace=True)
#     # use previous correct value to fillna
#     df.fillna(method="ffill", inplace=True)
#     df = df[['date', 'hour', 'REPORT_TYPE', 'HourlyDryBulbTemperature',
#     'HourlyRelativeHumidity', 'HourlyPrecipitation', 'HourlyWindSpeed']]
#     df.to_csv(weather_output_csv)
#     print(f"Dataframe Shape: {df.shape}")
#     print(f"Dataframe Preview: {df.head(5)}")

def extract_and_transform_covid(covid_output_csv):
    # raw data from nychealth
    url = 'https://raw.githubusercontent.com/nychealth/coronavirus-data/master/trends/caserate-by-modzcta.csv'
    # get webpage
    res = requests.get(url, allow_redirects=True)

    # save page info to file
    with open('data/raw_covid_data.csv','wb') as file:
        file.write(res.content)

    # read file
    df = pd.read_csv('data/raw_covid_data.csv')

    # melt data with rows containing week_ending(date), zip_zode, and case_rate
    df = df.melt(id_vars=["week_ending"],
        var_name="zip_code",
        value_name="case_rate")

    # slice the first 9 letters of column
    df["zip_code"]= df["zip_code"].str[9:]

    # save to csv
    df.to_csv(covid_output_csv)
    print(f"Dataframe Shape: {df.shape}")
    print(f"Dataframe Preview: {df.head(5)}")
    print(f"Missing Values: {df.isna().sum()}")


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

# transform_weather = PythonOperator(
#     task_id="transform_weather",
#     python_callable=extract_and_transform_weather,
#     op_kwargs={
#         "weather_csv": "data/nyc_daily_weather.csv",
#         "weather_output_csv": "data/transformed_nyc_daily_weather.csv"
#     },
#     dag=dag
# )

transform_covid = PythonOperator(
    task_id="transform_covid",
    python_callable=extract_and_transform_covid,
    op_kwargs={
        "covid_output_csv": "data/transformed_covid_data_table.csv"
    },
    dag=dag
)