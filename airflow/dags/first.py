from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Dayal',
    'depends_on_past': False,
    'email': ['dayal@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    logging.info("performing scraping")

def process():
    logging.info("performing processing")

def save():
    logging.info("performing saving")

with DAG(
    'Tolerante_a_fallas',
    default_args=default_args,
    description='Ejemplo de Airflow, Tolerante a fallas',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    get_data_task = PythonOperator(task_id="get", python_callable=get_data)
    process_data_task = PythonOperator(task_id="process", python_callable=process)
    save_data_task = PythonOperator(task_id="save", python_callable=save)

    get_data_task >> process_data_task >> save_data_task
