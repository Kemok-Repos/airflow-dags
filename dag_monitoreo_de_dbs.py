from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
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
    dag_id='Monitoreo_de_bases_de_datos',
    description="Identificar y erradicar queries con problemas.",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='20 * * * *',
    max_active_runs=1,
    catchup=False,
    tags=['monitoreo'],
) as dag:
    t1 = PythonOperator(
        task_id='Revision_de_queries_lentos',
        python_callable=check_slow_queries,
    )

