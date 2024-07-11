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

def execute_pipeline(command):
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            logging.info(f"Output:\n{stdout.decode()}")
        else:
            logging.error(f"Error:\n{stderr.decode()}")
            raise Exception(f"Command failed with exit code {process.returncode}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

dag = DAG(
    'pipeline3',
    default_args=default_args,
    description='Run pipeline1 followed by pipeline2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 4),
    catchup=False,
)

task_part1 = PythonOperator(
    task_id='execute_pipeline1',
    python_callable=execute_pipeline,
    op_kwargs={'command': 'cd /code-challenge/project && meltano schedule run pipeline1'},
    dag=dag,
)

task_part2 = PythonOperator(
    task_id='execute_pipeline2',
    python_callable=execute_pipeline,
    op_kwargs={'command': 'cd /code-challenge/project && meltano schedule run pipeline2'},
    dag=dag,
)

task_part1 >> task_part2