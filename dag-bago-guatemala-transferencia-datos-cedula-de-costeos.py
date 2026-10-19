from airflow import DAG
from datetime import datetime
from core_transfer import TransferTasks

cliente = 'bago guatemala'

default_args = {
    'owner': 'airflow',
    'email': ['emilianni@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0   
}
with DAG(
    dag_id='dag-bago-guatemala-transferencia-datos-cedula-de-costeos',
    description="Transferencia de los datos para cédula de costeos",
    default_args=default_args,    
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,       
    tags=['bago guatemala', 'transferencia'],
) as dag:

    t1 = TransferTasks(client=cliente, condition='preprocessing_cedula').task_groups()


