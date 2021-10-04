from airflow import DAG
from datetime import timedelta, datetime
from core_initialize import dag_init
from core_transfer import TransferTasks
from core_processing import build_processing_tasks
from core_notifications import NotificationTasks
from core_finale import dag_finale

cliente = 'kemok bi'

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
    dag_id=cliente.replace(' ', '-')+'-extraccion-y-procesamiento-completo-para-actualizacion-de-tableros',
    description="Extraer información y procesarla",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='45 11 * * *',
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = TransferTasks(client=cliente, condition='preprocessing').task_groups()
    t3 = build_processing_tasks(connection_id=cliente.replace(' ', '_')+'_postgres', repo='kemok-sql/sql/')
    
    tn1 = NotificationTasks(client=cliente)
    
    t4 = tn1.branch(7)
    t5 = tn1.tasks(7)

    t7 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})

    t1 >> t2 >> t3[0] 
    t3[-1] >> t4 >> t5 >> t7