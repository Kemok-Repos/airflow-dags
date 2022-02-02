from airflow import DAG
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

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
        sql="SELECT True;",
        follow_task_ids_if_true='Transferir-datos-desde-bago-carica-bi-hacia-app',
        follow_task_ids_if_false='Data-no-lista',
        conn_id='bago_caricam_app'
    )
    t2 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Transferir-datos-desde-bago-carica-bi-hacia-app',
        connection_id='8ba4b7bd-4017-47c8-b3b2-5d8964f8910f',
        asynchronous=True,
    )

    t3 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verificar-transferencia-de-datos',
        airbyte_job_id=t2.output,
    )

    t0 = DummyOperator(task_id='Data-no-lista')

    t1 >> t2 
    t1 >> t0