from airflow import DAG
from core_processing import build_processing_tasks
from datetime import datetime, timedelta
from os import getcwd

default_args = {
    'owner': 'airflow',
    'email': ['wilmer@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id='bago-caricam-actualizacion-mensual-de-aplicacion-de-proyecciones-de-venta-5-meses',
    description="Extracción desde Senz a Senz Viz (5 meses)",
    default_args=default_args,
    schedule_interval='5 0 1 * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago caricam', 'transformacion'],
) as dag:

    t1 = build_processing_tasks(connection_id='bago_caricam_app_dev', repo='bago-caricam-sql/actualizar-mes-en-app-prueba')

