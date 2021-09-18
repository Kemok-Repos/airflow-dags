from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from core_transfer import TransferTasks

cliente = 'sr tendero'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=5)
}
with DAG(
    dag_id='extraccion_reporte_diario_'+cliente.replace(' ', '_'),
    description="Extrae la información necesaria para el reporte diario",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0,30 13-23,0-5 * * *',
    catchup=False,
    tags=['transferencia', cliente],
) as dag:

    t1 = TransferTasks(client=cliente, condition='hourly').task_groups()



