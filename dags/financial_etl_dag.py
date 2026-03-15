from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adding scripts folder to path so Airflow can find your logic
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))
from generator import generate_bulk_data
from filter import run_filter
from warehouse import run_warehouse

default_args = {
    'owner': 'Ranjith',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'financial_batch_pipeline',
    default_args=default_args,
    description='End-to-End Medallion Architecture Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 1),
    catchup=False
) as dag:

    generate_step = PythonOperator(
        task_id='generate_raw_data',
        python_callable=generate_bulk_data
    )

    filter_step = PythonOperator(
        task_id='validate_and_filter',
        python_callable=run_filter
    )

    warehouse_step = PythonOperator(
        task_id='load_to_duckdb_warehouse',
        python_callable=run_warehouse
    )

    # Setting the dependencies
    generate_step >> filter_step >> warehouse_step
