from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=10)    
}

with DAG(
  dag_id="kontainer_actualizar_url_params",
  description="Actualiza los filtros de los tableros para que vean el mes actual",
  default_args=default_args,
  start_date=days_ago(3),
  schedule_interval="5 6 1 * *",
  catchup=False,
  tags=['herramienta','kontainer'],

) as dag:

  find_extraction_date = PostgresOperator(
    task_id="actualizar_url_params",
    postgres_conn_id='kontainer_postgres',
    sql='sql/refresh-url-params.sql'
  )
