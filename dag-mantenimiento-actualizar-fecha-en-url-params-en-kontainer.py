from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=10)
}

with DAG(
        dag_id="mantenimiento-actualizar-fecha-en-url-params-en-kontainer",
        description="Actualiza los filtros de los tableros para que vean el mes actual",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        schedule_interval="0 13 1 * *",
        catchup=False,
        tags=['herramienta', 'kontainer'],

) as dag:
    find_extraction_date = PostgresOperator(
        task_id="actualizar_url_params",
        postgres_conn_id='kontainer_postgres',
        sql='sql/refresh-url-params.sql'
    )
