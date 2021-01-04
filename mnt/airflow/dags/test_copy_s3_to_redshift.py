from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator

default_args = {
    'owner': 'johnrick',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('copy_s3_to_redshift',
          default_args=default_args,
          description='create tables for s3 data'#,
        #   schedule_interval='0 * * * *'
        )

# dates_to_redshift = StageToRedshiftOperator(
#     task_id='stage_dates',
#     dag=dag,
#     conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     s3_bucket='ud-covid-citibike',
#     s3_key = 'dates',    
#     table="dates",
#     file_format='CSV'
# )

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

# DROP TABLE IF EXISTS public.covid;
# DROP TABLE IF EXISTS public.bike;
# DROP TABLE IF EXISTS public.weather;
# DROP TABLE IF EXISTS public.stations;
# DROP TABLE IF EXISTS public.dates;