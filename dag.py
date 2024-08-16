from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from fetch_api_data import fetch_data
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'omkar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'news_api_snowflake',
    default_args=default_args,
    description='Fetch data from API, save as Parquet to GCS, and process in Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 8, 16),
    catchup=False
)

fetch_data_task = PythonOperator(
    task_id='api_data_to_gcs',
    python_callable=fetch_data,
    dag=dag
)

snowflake_create_table = SnowflakeOperator(
    task_id='create_snowflake_data',
    sql="""
        CREATE TABLE IF NOT EXISTS news_api_data USING TEMPLATE(
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE (
        INFER_SCHEMA(
            LOCATION => '@project.PUBLIC.NEWS_RAW_DATA_STAGE',
            FILE_FORMAT => 'PARQUET_FORMAT'
        )))
        """,
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

snowflake_insert_data = SnowflakeOperator(
    task_id='insert_data_to_table',
    sql="""
        COPY INTO news_api_data FROM @project.PUBLIC.NEWS_RAW_DATA_STAGE
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
        """,
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

fetch_data_task >> snowflake_create_table >> snowflake_insert_data
