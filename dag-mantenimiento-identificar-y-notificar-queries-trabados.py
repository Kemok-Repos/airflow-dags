from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from core_notifications import NotificationTasks
from utils import check_slow_queries

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_failure': False,
    'sla': timedelta(minutes=10)
}

with DAG(
    dag_id='mantenimiento-identificar-y-notificar-queries-trabados',
    description="Identificar y erradicar queries con problemas.",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='20 * * * *',
    max_active_runs=1,
    catchup=False,
    tags=['monitoreo'],
) as dag:
    t1 = PythonOperator(
        task_id='Revision_de_queries_lentos',
        python_callable=check_slow_queries,
    )
    nt1 = NotificationTasks(client='kat')

    t2 = nt1.branch(1)
    t3 = nt1.tasks(1)

    t1 >> t2 >> t3
