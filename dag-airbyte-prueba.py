
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
    dag_id='sr-tendero-extraccion-en-airbyte',
    description="Extracción en el ambiente de desarrollo de airbyte.",
    default_args=default_args,
    schedule_interval='0 15 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['sr tendero', 'airbyte'],
) as dag:

    t1 = AirbyteTriggerSyncOperator(
        airbyte_conn_id='airbyte_api',
        task_id='Inicia_trasferencia_de_datos_en_airbyte',
        connection_id='bd0ead52-f93b-414e-a184-78cbbd6ef333',
        asynchronous=True,
    )

    t2 = AirbyteJobSensor(
        airbyte_conn_id='airbyte_api',
        task_id='Verifica_la_finalizacion_de_la_transferencia',
        airbyte_job_id=t1.output,
    )


