import datetime as dt
import pandas as pd


from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import typing
import os

from orchestration.tasks.nba import functions as nba_functions



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': dt.datetime(2021, 1, 1),
    'retry_delay': dt.timedelta(minutes=5)
}

@dag(
    dag_id='nba_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['nba'],
    dagrun_timeout=dt.timedelta(minutes=60),
    max_active_runs=1
)
def nba_dag():

    @task(task_id='fetch_static_data')
    def fetch_static_data():
        nba_functions.static_data_fetch()

    @task(task_id='all_time_leaders')
    def all_time_leaders():
        nba_functions.all_time_leaders()

    @task(task_id='get_games')
    def extract_games() -> typing.List[typing.Dict[str, pd.DataFrame]]:
        data = nba_functions.get_games()
        return data
    
    @task(task_id='process_games')
    def process_games_data(teams_df_lizt: typing.List[typing.Dict[str, pd.DataFrame]]) -> None:
        nba_functions.process_games(teams_df_list=teams_df_lizt)


    [fetch_static_data(), all_time_leaders()] >> process_games_data(extract_games())

dag = nba_dag() 