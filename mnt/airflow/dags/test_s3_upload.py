from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import pandas as pd
import requests

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


default_args = {
    'owner': 'johnrick',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('test_load_to_s3',
          default_args=default_args,
          description='load data to s3'#,
        #   schedule_interval='0 * * * *'
        )

def upload_file(filename, key, bucket_name):
    hook = S3Hook('my_s3_conn')
    print(f"Bucket Name Exists: {hook.check_for_bucket(bucket_name)}")
    print(f"Bucket Prefixes: {hook.list_prefixes(bucket_name)}")
    print(f"Bucket Keys List: {hook.list_keys(bucket_name)}")
    hook.load_file(filename, key, bucket_name)
    print(f'Uploaded {filename} to {bucket_name} with {key}')
    # s3.Object(bucket_name, filename).put(Body=open())
    

upload_covid_to_s3 = PythonOperator(
    task_id = 'upload_covid_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/sample_transformed_covid.csv',
        'bucket_name': 'ud-covid-citibike',
        'key': 'covid'
    },
    dag=dag
)

upload_dates_to_s3 = PythonOperator(
    task_id = 'upload_dates_to_s3',
    python_callable=upload_file,
    op_kwargs={
        'filename': 'data/sample_transformed_date.csv',
        'bucket_name': 'ud-covid-citibike', #s3://skuchkula-etl/unique_valid_searches_
        'key': 'dates'
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
        'filename': 'data/sample_transformed_weather.csv',
        'bucket_name': 'ud-covid-citibike', 
        'key': 'weather'
    },
    dag=dag
)