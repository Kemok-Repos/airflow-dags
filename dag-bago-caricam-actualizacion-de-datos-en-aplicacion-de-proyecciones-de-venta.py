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
    'email': ['kevin@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id='bago-caricam-actualizacion-de-datos-en-aplicacion-de-proyecciones-de-venta',
    description="Extracción desde Senz a Senz Viz",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago caricam', 'airbyte'],
) as dag:

    t1 = BranchSQLOperator(
        task_id='Revision-de-datos',
        sql="SELECT bool_and(resultado) FROM tests;",
        follow_task_ids_if_true='Transferir-datos-desde-bi-hacia-app',
        follow_task_ids_if_false='Data-no-lista',
        conn_id='bago_caricam_postgres'
    )
    t2 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Transferir-datos-desde-bi-hacia-app',
        connection_id='b31f8d1c-a863-4d9c-a02e-69ca45180e7a',
        asynchronous=True,
    )

    t3 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verificar-transferencia-de-datos',
        airbyte_job_id=t2.output,
    )

    t4 = build_processing_tasks(connection_id='bago_caricam_app', repo='bago-caricam-sql/actualizar-tablas-en-app')

    t0 = DummyOperator(task_id='Data-no-lista')

    t1 >> t2 
    t1 >> t0
    t3 >> t4[0]