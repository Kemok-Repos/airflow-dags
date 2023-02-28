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
    dag_id='sr-tendero-extraccion-cada-media-hora-tiendas-pos',
    description="Extracción desde Tendero_Pos as SrTendero cada media hora",
    default_args=default_args,
    schedule_interval='*/25 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['sr tendero'],
) as dag:

    t1 = build_processing_tasks(connection_id='sr_tendero_postgres', repo='sr-tendero-sql/metricas_diarias-tiendas-pos')

