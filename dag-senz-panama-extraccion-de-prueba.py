from airflow import DAG
from datetime import datetime
from core_transfer import TransferTasks

cliente = 'senz pa'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries': 0
}
with DAG(
    dag_id=cliente.replace(' ', '-')+'-extraccion-de-prueba',
    description="Prueba de extracción",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['transferencia', cliente],
) as dag:

    t1 = TransferTasks(client=cliente, condition='test').task_groups()
