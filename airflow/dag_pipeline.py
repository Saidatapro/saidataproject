
# Apache Airflow DAG to schedule ETL
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

default_args = {
    'owner': 'sai_teja',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

def run_etl():
    os.system("python3 /home/replit/etl/transform.py")

with DAG('ecommerce_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_transformation',
        python_callable=run_etl
    )
