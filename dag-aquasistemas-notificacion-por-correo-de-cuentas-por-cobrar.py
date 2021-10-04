from airflow import DAG
from core_notifications import NotificationTasks
from datetime import timedelta, datetime

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
    dag_id=cliente.replace(' ', '-')+'-notificacion-por-correo-de-cuentas-por-cobrar',
    description="Activación asincrona del envío de recordatorios y alertas de pago para Aquasistemas.",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['aquasistemas', 'comunicación', 'kontact'],
) as DAG:

    nt1 = NotificationTasks(conn_id=conn_id)
    t1 = nt1.tasks(5)
