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
    dag_id='dag-bago-guatemala-actualizar-maestros-cupones',
    description="Actualizar tablas maestro en la base de datos de cupones producción",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago guatemala', 'transformacion'],
) as dag:

    t1 = build_processing_tasks(connection_id='bago_guatemala_cupones', repo='bago-guatemala-sql/cupones')

