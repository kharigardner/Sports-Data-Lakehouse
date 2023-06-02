from datetime import datetime, timedelta

from tasks.nfl import functions as nfl_func

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import typing
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2021, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='nfl_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['nfl'],
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    params={'year': datetime.now().year}
)
def nfl_dag():
    
    @task(task_id='fetch_static_data')
    def fetch_static_data():
        nfl_func.static_data_fetch()

    yearly_data_task = PythonOperator(
        task_id = 'fetch_yearly_data',
        python_callable = nfl_func.fetch_yearly_data,
        op_kwargs={'year': "{{ params.year }}"}
    )


    fetch_static_data() >> yearly_data_task 

dag = nfl_dag() # type: ignore