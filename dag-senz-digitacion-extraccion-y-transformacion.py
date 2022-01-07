
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
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
    dag_id='senz-digitacion-extraccion-y-transformacion',
    description="Extracción desde Senz a Digitación Senz",
    default_args=default_args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz', 'airbyte'],
) as dag:

    t1 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Transferir-datos-desde-senz-gt',
        connection_id='7b5cdd2e-3a31-47db-a560-6b27415e83ac',
        asynchronous=True,
    )

    t2 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verificar-datos-desde-senz-gt',
        airbyte_job_id=t1.output,
    )
