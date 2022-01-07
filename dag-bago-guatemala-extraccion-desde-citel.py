from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

cliente = 'bago guatemala'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=90)
}
with DAG(
    dag_id=cliente.replace(' ', '-')+'-extraccion-desde-citel',
    description="Extraer información y procesarla",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t0 = SSHOperator(
        task_id='correr-script-de-nelson-en-servidor',
        command='cd /root/bago-guatemala-nodejs/ ; node index.js &',
        ssh_conn_id=cliente.replace(' ', '_')+'_server',
        conn_timeout=None,
        cmd_timeout=36000
    )
