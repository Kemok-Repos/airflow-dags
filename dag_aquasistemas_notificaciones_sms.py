from airflow import DAG
from core_notifications import message_tasks
from airflow.utils.dates import days_ago
from datetime import timedelta

cliente = 'aquasistemas'
conn_id = cliente.replace(' ', '_')+'_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_success':  True,
    'email_on_failure': True,
    'retries': 0   
}

with DAG(
    dag_id='envio_de_sms_de_recordatorio_de_pago_aquasistemas',
    description="Activación asincrona del envío de mensajes de texto para Aquasistemas.",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['aquasistemas', 'comunicación', 'kontact'],
) as DAG:

    t1 = message_tasks(6, conn_id)
