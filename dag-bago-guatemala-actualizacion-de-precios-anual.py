from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    dag_id='bago-guatemala-actualizacion-de-precios-anual',
    description="Actualiza la tabla de auxiliar_precios",
    default_args=default_args,
    schedule_interval='5 6 31 1 *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['bago guatemala'],
) as dag:

    t1 = PostgresOperator (
        task_id='Actualizar-tabla-auxiliar_precios',
        postgres_conn_id='bago_guatemala_postgres',
        sql="bago-guatemala-sql/actualizar-precios-para-el-anio.sql"
    )
