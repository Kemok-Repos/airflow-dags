from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta, datetime
from core_initialize import dag_init
from core_transfer import TransferTasks
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'senz gt dbmart'

default_args = {
    'owner': 'airflow',
    'email': ['emilianni@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'sla': timedelta(minutes=10)
}
with DAG(
    dag_id=cliente.replace(' ', '-')+'-refresh-clientes-recurrentes',
    description="Ejecuta el refresh diario por tipo de vista tomando en cuenta si hubo acceso del cliente a sus tablero",
    default_args=default_args,
    start_date=datetime(2022, 10, 7),
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)   
    t3 = build_processing_tasks(client=cliente)
    t4 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})
    t1 >> t2 >> t3 >> t4[0] 
    t4[-1] >> t5