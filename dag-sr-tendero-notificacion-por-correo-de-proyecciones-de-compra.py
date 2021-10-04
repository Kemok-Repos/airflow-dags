from airflow import DAG
from core_notifications import NotificationTasks
from datetime import timedelta, datetime

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
    dag_id=cliente.replace(' ', '-')+'-notificacion-por-correo-de-proyecciones-de-compra',
    description="Envio de estadisticas de stock de Sr. Tendero.",
    default_args=default_args,
    schedule_interval='0 13 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sr tendero', 'comunicación', 'kontact'],
) as dag:

    t1 = NotificationTasks(client=cliente).tasks(5)
