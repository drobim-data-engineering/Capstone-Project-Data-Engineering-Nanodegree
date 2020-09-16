from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFilesToS3Operator, CheckS3FileCount

# Define S3 Bucket

datalake_bucket_name = 'us-accidents-datalake'

# Define Default DAG Args

default_args = {
    'owner': 'drobim',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}

# Create DAG

dag = DAG('create_datalake_dag',
          default_args=default_args,
          description='Load data into raw S3 datalake.',
          catchup=False
          )

# Create Tasks

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id='create_raw_datalake',
    bucket_name=datalake_bucket_name,
    dag=dag
)

upload_covid_data = UploadFilesToS3Operator(
    task_id='upload_covid_data',
    bucket_name=datalake_bucket_name,
    path='/usr/local/data/split/covid-19/',
    dag=dag
)

upload_demographic_data = UploadFilesToS3Operator(
    task_id='upload_demographic_data',
    bucket_name=datalake_bucket_name,
    path='/usr/local/data/split/us-cities-demographics/',
    dag=dag
)

upload_accident_data = UploadFilesToS3Operator(
    task_id='upload_accident_data',
    bucket_name=datalake_bucket_name,
    path='/usr/local/data/split/us-accidents/',
    dag=dag
)

check_file_quantity = CheckS3FileCount(
    task_id='check_file_quantity',
    bucket_name=datalake_bucket_name,
    expected_count=12,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG dependencies

start_operator >> create_raw_datalake

create_raw_datalake >> [upload_accident_data, upload_covid_data, upload_demographic_data]

[upload_accident_data, upload_covid_data, upload_demographic_data] >> check_file_quantity

check_file_quantity >> end_operator