#DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
from script.extract_data import extract_data
from script.transform_data import transform_data
from script.load_data import load_data
from script.validate_data import validate_data
from script.utils import send_slack_message


# Default args
default_args = {
'owner': 'abhiraj',
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'retry_exponential_backoff': True,
'max_retry_delay': timedelta(minutes=30),
}


with DAG(
    dag_id="retail_sales_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 10),
    catchup=False,
    default_args=default_args,
    tags=["retail", "pyspark", "etl"]
) as dag: