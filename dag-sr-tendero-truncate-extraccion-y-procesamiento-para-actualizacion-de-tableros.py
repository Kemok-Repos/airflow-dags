from airflow import DAG
from datetime import timedelta, datetime
from core_initialize import dag_init
from core_transfer import TransferTasks
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'sr tendero'

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
    dag_id=cliente.replace(' ', '-')+'-truncate-extraccion-y-procesamiento-para-actualizacion-de-tableros',
    description="Extraer información y procesarla",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = PostgresOperator (
        task_id='Truncar-tablas-g2s',
        postgres_conn_id='sr_tendero_postgres',
        sql="sr-tendero-sql/g2s_truncate.sql"
    )

    t3 = TransferTasks(client=cliente, condition='preprocessing').task_groups()
    t4 = build_processing_tasks(client=cliente)
    t5 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})
    t1 >> t2 >> t3 >> t4[0] 
    t4[-1] >> t5
