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

default_args = {
    'owner': 'johnrick',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('test_quality_dag',
          default_args=default_args,
          description='load data to pyspark'
        #   schedule_interval='0 * * * *'
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

run_quality_checks