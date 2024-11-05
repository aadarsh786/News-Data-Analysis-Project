
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from prac2 import fetch
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

def_arg = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'newsapi_to_gcs',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=def_arg,
    start_date=datetime(2024, 11, 5)
    
    
)

# Tasks
pythn_operator = PythonOperator(
    task_id='fetching_data',  
    python_callable=fetch,
    dag=dag
)

snowflake_operator1 = SnowflakeOperator(
    task_id='create_destination_table',  
    
    sql="""CREATE TABLE IF NOT EXISTS news_api_data USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage',
                    FILE_FORMAT => 'parquet_format'
                ))
            )""",
    snowflake_conn_id="snowflake_conn"
               
                        
)

snowflake_operator2 = SnowflakeOperator(
    task_id='copy_data_from_stage_to_destination', 

 sql="""COPY INTO news_api.PUBLIC.news_api_data 
            FROM @news_api.PUBLIC.gcs_raw_data_stage
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
            FILE_FORMAT = (FORMAT_NAME = 'parquet_format') 
            """,
    snowflake_conn_id="snowflake_conn"
      
    
)

pythn_operator >> snowflake_operator1 >> snowflake_operator2
