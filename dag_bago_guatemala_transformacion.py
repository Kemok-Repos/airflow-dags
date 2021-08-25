from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from transfer_manager import build_transfer_tasks, check_transfer_tasks, build_processing_tasks
import pandas as pd
import config

CONN = 'bago_guatemala_postgres'

REPO = 'bago-guatemala-sql/sql/'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)    
}
with DAG(
  dag_id="transformacion_bago_guatemala",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['bago_guatemala','transformacion'],
) as dag:
    # Leer el listado de tareas de transformación
    tg1 = build_processing_tasks(CONN, REPO)

    t2 = TelegramOperator(
    task_id = 'Notificar_errores_de_procesamiento_a_soporte',
    telegram_conn_id='soporte2_telegram',
    trigger_rule='all_failed',
    text = config.ALERTA_FALLA
    )

    tg1[-1] >> t2