from airflow import DAG
from core_processing import build_processing_tasks
from datetime import datetime, timedelta
from os import getcwd

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
    dag_id='dag-bago-guatemala-activar-desactivar-tableros-aims',
    description="Permite activar o desactivar tableros a los aims",
    default_args=default_args,
    schedule_interval='5 0 1 * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago guatemala', 'transformacion'],
) as dag:

    t1 = build_processing_tasks(connection_id='bago_metabase_postgres', repo='bago-guatemala-sql/activar-desactivar-tableros')

