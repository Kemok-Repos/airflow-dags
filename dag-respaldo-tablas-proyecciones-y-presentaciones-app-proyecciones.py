from airflow import DAG
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.dummy import DummyOperator
from core_processing import build_processing_tasks
from datetime import datetime, timedelta
from os import getcwd

PATH = getcwd() + '/dags/'

default_args = {
    'owner': 'airflow',
    'email': ['emilianni@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id='bago-caricam-respaldo-tablas-proyecciones-y-presentaciones',
    description="Respalda dos tablas de la app proyecciones",
    default_args=default_args,
    schedule_interval=None,
    schedule_interval='0 9 30 * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago caricam', 'airbyte'],
) as dag:

    t1 = build_processing_tasks(connection_id='bago_caricam_app', repo='bago_caricam_sql/respaldo-tablas-app-proyecciones')                                                                       

    t1 