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
        dag_id="senz-digitacion-actualizar-estados-de-concursos",
        description="Actualiza los estados de consursos de Guatecompras para homologarlos",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['transformacion', 'digitacion senz'],

) as dag:
    t1 = PostgresOperator(
        task_id="Actualizar_estados_de_concursos",
        postgres_conn_id='digitacion_senz_postgres',
        sql='SELECT f_insertar_nuevos_estatus();'
    )
