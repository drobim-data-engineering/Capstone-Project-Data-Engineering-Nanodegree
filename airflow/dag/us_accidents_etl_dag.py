from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import S3ToRedshiftOperator, LoadTableOperator, DataQualityOperator
from helpers.sql_create import SqlCreate
from helpers.sql_load import SqlLoad
from create_datalake_dag import datalake_bucket_name

# Changes Operator UI Color

PostgresOperator.ui_color = '#F98866'

# Define Data Quality Checks

data_quality_args = [
        {
            'sql': 'SELECT COUNT(*) FROM staging.us_covid_19;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM staging.us_demographics;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM staging.us_accidents;',
            'op': 'gt',
            'val': 0
        }
    ]

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

dag = DAG('us_accident_etl_dag',
          default_args=default_args,
          description='Extract, Load and Transform Data from S3 to Redshift',
          catchup=False
          )

# Create Tasks

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_staging_schema = PostgresOperator(
    task_id='create_staging_schema',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_staging_schema
)

create_dim_schema = PostgresOperator(
    task_id='create_dim_schema',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_dim_schema
)

create_fact_schema = PostgresOperator(
    task_id='create_fact_schema',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_fact_schema
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

create_table_staging_covid_19 = PostgresOperator(
    task_id='create_table_staging_covid_19',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_staging_covid_19
)

create_table_staging_us_demographics = PostgresOperator(
    task_id='create_table_staging_us_demographics',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_staging_us_demographics
)

create_table_staging_us_accidents = PostgresOperator(
    task_id='create_table_staging_us_accidents',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_staging_us_accidents
)

create_table_dim_dates = PostgresOperator(
    task_id='create_table_dim_dates',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_dim_dates
)

create_table_dim_accident_address = PostgresOperator(
    task_id='create_table_dim_accident_address',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_dim_accident_address
)

create_table_dim_accident_location_details = PostgresOperator(
    task_id='create_table_dim_accident_location_details',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_dim_accident_location_details
)

create_table_dim_weather_condition = PostgresOperator(
    task_id='create_table_dim_weather_condition',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_dim_weather_condition
)

create_table_fact_us_demographics = PostgresOperator(
    task_id='create_table_fact_us_demographics',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_fact_us_demographics
)

create_table_fact_covid_19 = PostgresOperator(
    task_id='create_table_fact_covid_19',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_fact_covid_19
)

create_table_fact_us_accidents = PostgresOperator(
    task_id='create_table_fact_us_accidents',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlCreate.create_table_fact_us_accidents
)

tables_created = DummyOperator(task_id='tables_created', dag=dag)

load_staging_us_accidents_table = S3ToRedshiftOperator(
    task_id='load_staging_us_accidents_table',
    dag=dag,
    s3_bucket=datalake_bucket_name,
    s3_prefix='us-accidents',
    table='staging.us_accidents',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='truncate'
)

load_staging_us_covid_19_table = S3ToRedshiftOperator(
    task_id='load_staging_us_covid_19_table',
    dag=dag,
    s3_bucket=datalake_bucket_name,
    s3_prefix='covid-19',
    table='staging.us_covid_19',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='truncate'
)

load_staging_us_demographics_table = S3ToRedshiftOperator(
    task_id='load_staging_us_demographics_table',
    dag=dag,
    s3_bucket=datalake_bucket_name,
    s3_prefix='us-cities-demographics',
    table='staging.us_demographics',
    copy_options="FORMAT as CSV DELIMITER as ';' QUOTE as '\"' IGNOREHEADER 1",
    mode='truncate'
)

load_accident_location_details_dim_table = LoadTableOperator(
    task_id='load_accident_location_details_dim_table',
    dag=dag,
    table='dim.accident_location_details',
    select_sql=SqlLoad.insert_acc_location_details_table,
    mode='truncate'
)

load_weather_condition_dim_table = LoadTableOperator(
    task_id='load_weather_condition_dim_table',
    dag=dag,
    table='dim.weather_condition',
    select_sql=SqlLoad.insert_wth_condition_table,
    mode='truncate'
)

load_accident_address_dim_table = LoadTableOperator(
    task_id='load_accident_address_dim_table',
    dag=dag,
    table='dim.accident_address',
    select_sql=SqlLoad.insert_acc_address_table,
    mode='truncate'
)

load_dates_dim_table = LoadTableOperator(
    task_id='load_dates_dim_table',
    dag=dag,
    table='dim.dates',
    select_sql=SqlLoad.insert_dates_table,
    mode='truncate'
)

load_demographics_fact_table = LoadTableOperator(
    task_id='load_demographics_fact_table',
    dag=dag,
    table='fact.us_demographics',
    select_sql=SqlLoad.insert_demographics_table,
    mode='truncate'
)

load_covid19_fact_table = LoadTableOperator(
    task_id='load_covid19_fact_table',
    dag=dag,
    table='fact.us_covid_19',
    select_sql=SqlLoad.insert_covid_table,
    mode='truncate'
)

load_accident_fact_table = LoadTableOperator(
    task_id='load_accident_fact_table',
    dag=dag,
    table='fact.us_accidents',
    select_sql=SqlLoad.insert_accident_table,
    mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_stmts=data_quality_args
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG dependencies

start_operator >> [create_staging_schema,create_dim_schema,create_fact_schema]

[create_staging_schema,create_dim_schema,create_fact_schema] >> schema_created

schema_created >> [create_table_staging_covid_19,
                   create_table_staging_us_demographics,
                   create_table_staging_us_accidents]

schema_created >> [create_table_dim_dates,
                   create_table_dim_accident_address,
                   create_table_dim_accident_location_details,
                   create_table_dim_weather_condition]

schema_created >> [create_table_fact_us_demographics,
                   create_table_fact_covid_19,
                   create_table_fact_us_accidents]

[create_table_staging_covid_19
,create_table_staging_us_demographics
,create_table_staging_us_accidents] >> tables_created

[create_table_dim_dates,
 create_table_dim_accident_address,
 create_table_dim_accident_location_details,
 create_table_dim_weather_condition] >> tables_created

[create_table_fact_us_demographics,
 create_table_fact_covid_19,
 create_table_fact_us_accidents] >> tables_created

tables_created >> [load_staging_us_accidents_table,load_staging_us_covid_19_table,load_staging_us_demographics_table]

load_staging_us_accidents_table >> [load_accident_location_details_dim_table
                                    ,load_weather_condition_dim_table
                                    ,load_accident_address_dim_table
                                    ,load_dates_dim_table
                                    ,load_accident_fact_table]

load_staging_us_covid_19_table >>  [load_covid19_fact_table]

load_staging_us_demographics_table >>  [load_demographics_fact_table,load_covid19_fact_table]

[load_accident_location_details_dim_table
,load_weather_condition_dim_table
,load_accident_address_dim_table
,load_dates_dim_table] >> run_quality_checks

[load_demographics_fact_table
,load_covid19_fact_table
,load_accident_fact_table] >> run_quality_checks

run_quality_checks >> end_operator