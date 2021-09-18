from airflow import DAG
from core_notifications import NotificationTasks
from airflow.utils.dates import days_ago
from datetime import timedelta

cliente = 'sr tendero'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'sla': timedelta(minutes=10)
}
with DAG(
    dag_id='envio_de_estadisticas_proyecciones_de_compra_sr_tendero',
    description="Envio de estadisticas de stock de Sr. Tendero.",
    default_args=default_args,
    schedule_interval='0 13 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['sr tendero', 'comunicación', 'kontact'],
) as dag:

    t1 = NotificationTasks(client=cliente).tasks(5)
