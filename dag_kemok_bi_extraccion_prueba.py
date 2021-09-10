from airflow import DAG
from airflow.utils.dates import days_ago
from core_transfer import build_transfer_tasks

cliente = 'kemok_bi'

conn_id = cliente.replace(' ', '_')+'_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries': 0
}
with DAG(
    dag_id='extraccion_de_prueba_'+cliente.replace(' ', '_'),
    description="Prueba de extracción",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['transferencia', cliente],
) as dag:

    t1 = build_transfer_tasks(conn_id, 'test')
