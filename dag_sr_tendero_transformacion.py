from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from core_initialize import dag_init
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'sr tendero'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id="transformacion-sr-tendero",
    description="Transforma la informacion cruda para el uso de las aplicaciones",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = build_processing_tasks(client=cliente)
    t3 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})
    t1 >> t2[0] 
    t2[-1] >> t3