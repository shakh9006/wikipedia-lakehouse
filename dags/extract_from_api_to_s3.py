import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/internal')

from config.index import DEFAULT_ARGS
from scripts.get_dates import get_dates
from scripts.fetch_latest_available import fetch_latest_available

DESCRIPTION = "Extract data from API to S3"
TAGS = ["extract", "api", "s3", "bronze"]

def extract_from_api_handler(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Extracting data from {start_date} to {end_date}")
    fetch_latest_available(start_date)

dag = DAG(
    dag_id="extract_from_api_to_s3",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 10),
    schedule="0 * * * *",
    catchup=False,
    tags=TAGS,
    description=DESCRIPTION,
    max_active_runs=1,
    max_active_tasks=1,
)

with dag:
    extract_from_api = PythonOperator(
        task_id="extract_from_api",
        python_callable=extract_from_api_handler,
    )

    extract_from_api