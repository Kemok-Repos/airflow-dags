from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from core_initialize import dag_init
from core_transfer import TransferTasks
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'bago caricam'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=90)
}
with DAG(
    dag_id='extraccion_completa_'+cliente.replace(' ', '_'),
    description="Extraer información y procesarla",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='30 8 * * *',
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = TransferTasks(client=cliente, condition='preprocessing').task_groups()
    t3 = build_processing_tasks(client=cliente)
    t4 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})
    t1 >> t2 >> t3[0] 
    t3[-1] >> t4