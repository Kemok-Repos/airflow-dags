
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
    dag_id='senz-viz-extraccion-y-transformacion',
    description="Extracción desde Senz a Senz Viz",
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz', 'airbyte'],
) as dag:

    t1 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Transferir-datos-desde-senz-gt',
        connection_id='721e3d3c-1f04-43ad-aa58-3143f7d5df63',
        asynchronous=True,
    )

    t2 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verificar-datos-desde-senz-gt',
        airbyte_job_id=t1.output,
    )

    t3 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Transferir-datos-desde-digitacion_senz',
        connection_id='721e3d3c-1f04-43ad-aa58-3143f7d5df63',
        asynchronous=True,
    )

    t4 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verificar-datos-desde-digitacion_senz',
        airbyte_job_id=t3.output,
    )

    t2 >> t3
