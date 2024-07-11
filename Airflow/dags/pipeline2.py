from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def execute_pipeline2():
    directory = r'C:\code-challenge\project'
    command = 'meltano schedule run pipeline2'
    try:
        result = subprocess.run(f'cd /d {directory} && {command}', shell=True, check=True, capture_output=True, text=True)
        logging.info(f"Output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"An error occurred: {e.stderr}")
        raise

dag = DAG(
    'pipeline2',
    default_args=default_args,
    description='Run only pipeline2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 4),
    catchup=False,
)

task_part2 = PythonOperator(
    task_id='execute_pipeline2',
    python_callable=execute_pipeline2,
    dag=dag,
)