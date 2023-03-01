from airflow import DAG
from datetime import timedelta, datetime
from core_processing import build_processing_tasks

default_args = {
    'owner': 'airflow',
    'email': ['wilmer@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id='fundacion-sonisas-para-refrescar-vistas-materializadas',
    description="Refrescar vistas materializadas",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['transferencia', 'procesamiento', 'fundacion'],
) as dag:

    t2 = build_processing_tasks(connection_id='fundacion_sonrisas_postgres', repo='fundacion-sonrisas-sql/sql')

    t2[0]

