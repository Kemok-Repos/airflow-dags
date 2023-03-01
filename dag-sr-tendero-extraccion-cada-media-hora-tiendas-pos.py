from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    schedule_interval='0,25 13-23,0-5 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['sr tendero'],
) as dag:
    t1 = PostgresOperator (
        task_id='extraer-metricas-diarias-tiendas-pos',
        postgres_conn_id='sr_tendero_postgres',
        sql="sr-tendero-sql/metricas_diarias-tiendas-pos.sql"
    )
    
    t1 

