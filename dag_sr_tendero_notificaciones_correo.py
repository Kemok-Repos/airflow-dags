from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from kontact.client import KontactClient
from airflow.utils.dates import days_ago
from utils import telegram_chat
from datetime import timedelta


def enviar_campanha():
    box = KontactClient('2efbc42c9f6311ea8c9b58fb844606cc')
    res = box.send_campaign(campaign_id=13)
    return res.json()


default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_success':  False,
    'email_on_failure': True,
    'email_on_retry':   True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=10)    
}

dag = DAG(
    dag_id='envio_de_estadisticas_proyecciones_de_compra_sr_tendero',
    description="Envio de estadisticas de stock de Sr. Tendero.",
    default_args=default_args,
    schedule_interval='0 13 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['sr-tendero', 'comunicación', 'kontact'],
)

t1 = PythonOperator(
    task_id='activar_campanha',
    python_callable=enviar_campanha,
    retries=0,
    dag=dag
)

t2 = TelegramOperator(
    task_id='notificacion_a_soporte',
    telegram_conn_id='soporte1_telegram',
    chat_id= telegram_chat(),
    text='NOTIF: Se activo el envío de campaña de correos para Sr. Tendero.',
    dag=dag
)

t1 >> t2
