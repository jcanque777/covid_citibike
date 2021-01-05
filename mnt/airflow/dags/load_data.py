import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook

import pandas as pd
import requests
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator
################################################################################
################################################################################

def extract_and_transform_weather(weather_csv, weather_output_csv):
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
    # use previous correct value to fillna
    df.fillna(method="ffill", inplace=True)
    df = df[['date', 'hour', 'REPORT_TYPE', 'HourlyDryBulbTemperature',
    'HourlyRelativeHumidity', 'HourlyPrecipitation', 'HourlyWindSpeed']]
    df[:50].to_csv(weather_output_csv, index=False)
    print(f"Dataframe Shape: {df.shape}")
    print(f"Dataframe Preview: {df.head(5)}")

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
    
    # ignore city names in zip code column
    df = df[df["zip_code"].str.startswith("1")]
    df['week_ending'] = pd.to_datetime(df['week_ending'])
    df.reset_index(inplace=True)
    df.drop(columns=["index"], inplace=True)

    # save to csv
    df[:50].to_csv(covid_output_csv, index=False)
    print(f"Dataframe Shape: {df.shape}")
    print(f"Dataframe Preview: {df.head(5)}")
    print(f"Missing Values: {df.isna().sum()}")

def extract_and_transform_citibike(raw_citibike_csv, citibike_output_csv):
    # read csv and save to df
    df = pd.read_csv(raw_citibike_csv)

    # turn stoptime column to timestamp from string
    df["stoptime_ts"] = 0
    df["stoptime_ts"] = pd.to_datetime(df.stoptime)

    cols_to_keep = ['stoptime_ts', 'start station name', 'end station name', 'bikeid', 'usertype', 'birth year', 'gender']

    df = df[cols_to_keep]

    # create date column and save
    df["date"] = df["stoptime_ts"].dt.date

    # create time column and save
    df["hour"] = df["stoptime_ts"].dt.hour

    # Group By Date, Hour, Station From, Station End, Bikeid Count
    df = df.groupby(["date", "hour", "start station name", "end station name"], as_index=False)["bikeid"].count()
    df.rename(columns={"bikeid":"ride_counts"}, inplace=True)
    df[:50].to_csv(citibike_output_csv, index=False)
    print(f"Dataframe Shape: {df.shape}")
    print(f"Dataframe Preview: {df.head(5)}")
    print(f"Missing Values: {df.isna().sum()}")


def upload_file(filename, key, bucket_name):
    hook = S3Hook('aws_credentials')
    print(f"Bucket Name Exists: {hook.check_for_bucket(bucket_name)}")
    print(f"Bucket Prefixes: {hook.list_prefixes(bucket_name)}")
    print(f"Bucket Keys List: {hook.list_keys(bucket_name)}")
    hook.load_file(filename, key, bucket_name)
    print(f'Uploaded {filename} to {bucket_name} with {key}')
    

################################################################################
################################################################################


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
          description='load data to pyspark',
          max_active_runs=1
        #   schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

transform_weather = PythonOperator(
    task_id="transform_weather",
    python_callable=extract_and_transform_weather,
    op_kwargs={
        "weather_csv": "data/nyc_daily_weather.csv",
        "weather_output_csv": "data/transformed_nyc_daily_weather.csv"
    },
    dag=dag
)

transform_covid = PythonOperator(
    task_id="transform_covid",
    python_callable=extract_and_transform_covid,
    op_kwargs={
        "covid_output_csv": "data/transformed_covid_data_table.csv"
    },
    dag=dag
)

transform_citibike = PythonOperator(
    task_id="transform_citibike",
    python_callable=extract_and_transform_citibike,
    op_kwargs={
        "raw_citibike_csv": "data/202011-citibike-tripdata.csv",
        "citibike_output_csv": "data/transformed_citibike_data.csv"
    },
    dag=dag
)

upload_dates_to_s3 = PythonOperator(
    task_id = 'upload_dates_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/sample_transformed_date.csv',
        'bucket_name': 'ud-covid-citibike', 
        'key': 'dates'
    },
    dag=dag
)

upload_covid_to_s3 = PythonOperator(
    task_id = 'upload_covid_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/transformed_covid_data_table.csv',
        'bucket_name': 'ud-covid-citibike', # added s3_bucket:ud-covid-citibike bucket variable in Airflow
        'key': 'covid'
    },
    dag=dag
)

upload_stations_to_s3 = PythonOperator(
    task_id = 'upload_stations_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/sample_transformed_stations.csv',
        'bucket_name': 'ud-covid-citibike', 
        'key': 'stations'
    },
    dag=dag
)

upload_weather_to_s3 = PythonOperator(
    task_id = 'upload_weather_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': "data/transformed_nyc_daily_weather.csv",
        'bucket_name': 'ud-covid-citibike', 
        'key': 'weather'
    },
    dag=dag
)

upload_bike_to_s3 = PythonOperator(
    task_id = 'upload_bike_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/transformed_citibike_data.csv',
        'bucket_name': 'ud-covid-citibike', 
        'key': 'citibike'
    },
    dag=dag
)

table_creation = PostgresOperator(
    task_id='tables_creation',
    dag=dag,
    postgres_conn_id='redshift',
    sql = '/create_tables.sql'
)

dates_to_redshift = StageToRedshiftOperator(
    task_id='stage_dates',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='ud-covid-citibike',
    s3_key = 'dates',    
    table="dates",
    file_format='CSV'
)

bike_to_redshift = StageToRedshiftOperator(
    task_id='bike_to_redshift',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='ud-covid-citibike',
    s3_key = 'citibike',    
    table="bike",
    file_format='CSV'
)

covid_to_redshift = StageToRedshiftOperator(
    task_id='covid_to_redshift',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='ud-covid-citibike',
    s3_key = 'covid',    
    table="covid",
    file_format='CSV'
)

weather_to_redshift = StageToRedshiftOperator(
    task_id='weather_to_redshift',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='ud-covid-citibike',
    s3_key = 'weather',    
    table="weather",
    file_format='CSV'
)

stations_to_redshift = StageToRedshiftOperator(
    task_id='stations_to_redshift',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='ud-covid-citibike',
    s3_key = 'stations',    
    table="stations",
    file_format='CSV'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id = "redshift",
    tables = ['stations',
                'weather',
                'covid',
                'bike',
                'dates']
)


middle_operator = DummyOperator(task_id='Middle', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> [transform_citibike, transform_weather, transform_covid]

[transform_citibike, transform_weather, transform_covid] >> middle_operator

middle_operator >> [upload_dates_to_s3, upload_covid_to_s3, upload_stations_to_s3, upload_weather_to_s3, upload_bike_to_s3]

[upload_dates_to_s3, upload_covid_to_s3, upload_stations_to_s3, upload_weather_to_s3, upload_bike_to_s3] >> table_creation

table_creation >> [dates_to_redshift, bike_to_redshift, covid_to_redshift, weather_to_redshift, stations_to_redshift] >> run_quality_checks

run_quality_checks >> end_operator