import sys
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow/internal')

from config.index import DEFAULT_ARGS

def test_handler():
    logging.info("Test handler running")

dag = DAG(
    dag_id="test",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 10),
    schedule="0 8 * * *",
    catchup=False,
    tags=["test"],
    description="Test DAG",
    max_active_runs=1,
    max_active_tasks=1,
)

with dag:
    start = EmptyOperator(task_id="start")
    test = PythonOperator(task_id="test", python_callable=test_handler)
    end = EmptyOperator(task_id="end")

    (
        start >> 
        test >> 
        end
    )