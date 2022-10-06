from airflow import DAG
from core_finale import dag_finale
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils import read_text

DAG_ID = 'senz-gt-refresh-clientes-recurrentes'


default_args = {
    'owner': 'airflow',
    'email': ['emilianni@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'conn_id': 'senz_gt_mart_postgres',  
    'pool': 'senz_gt', 
    'sla': timedelta(minutes=10)
}
with DAG(
    dag_id=DAG_ID,
    description="",
    default_args=default_args,
    schedule_interval='0 23 * * 0',
    start_date=datetime(2022, 3, 2),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz-gt'],
) as dag:
    t1 = PostgresOperator (
        task_id='Refresh vista analisis_precio',
        postgres_conn_id='senz_gt_mart_postgres',
        sql='SELECT refresh_analisis_precio_clientes_recurrentes();'
    )
    t2 = PostgresOperator (
        task_id='Refresh vista marketshare',
        postgres_conn_id='senz_gt_mart_postgres',
        sql='SELECT refresh_marketshare_clientes_recurrentes();'
    )
    t3 = PostgresOperator (
        task_id='Refresh vista npgs',
        postgres_conn_id='senz_gt_mart_postgres',
        sql='SELECT refresh_npgs_clientes_recurrentes();'
    )
     t4 = PostgresOperator (
        task_id='Refresh vista kpis_de_equipo',
        postgres_conn_id='senz_gt_mart_postgres',
        sql='SELECT refresh_kpis_de_equipo_clientes_recurrentes();'
    )
      t5 = PostgresOperator (
        task_id='Refresh vista eguimiento_concursos_no_ofertados',
        postgres_conn_id='senz_gt_mart_postgres',
        sql='SELECT refresh_seguimiento_concursos_no_ofertados_recurrentes();'
    )
   t1 >> t2 >> t3 >> t4 >> t5