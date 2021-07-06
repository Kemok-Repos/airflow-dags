"""
Ejemplo de tarea para envio de correos
"""

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='ejemplo_email',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=['ejemplo','comunicación'],
)

send_message_email_task = EmailOperator(
    task_id='send_message_email',
    to='kevin@kemok.io',
    subject='Alerta de airflow',
    html_content='templates/simple-email-example.html',
    #files='ejemplo.txt'
    dag=dag
)
