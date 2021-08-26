from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from utils import build_transfer_tasks, check_transfer_tasks, build_processing_tasks, telegram_chat
import pandas as pd
import config

CONN = 'bago_caricam_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=5)
}
with DAG(
  dag_id="extraccion_de_prueba_bago_caricam",
  description="Prueba de extracción",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval=None,
  catchup=False,
  tags=['bago-caricam', 'extraccion'],
) as dag:

    tg1, task_log, task_log_names  = build_transfer_tasks(CONN, 'test')