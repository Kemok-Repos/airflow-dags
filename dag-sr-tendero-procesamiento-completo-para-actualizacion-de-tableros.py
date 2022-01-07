from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import timedelta, datetime
from core_initialize import dag_init
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'sr tendero'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)
}
with DAG(
    dag_id="sr-tendero-procesamiento-completo-para-actualizacion-de-tableros",
    description="Transforma la informacion cruda para el uso de las aplicaciones",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = SSHOperator(
        task_id='correr-script-de-verificacion',
        command='cd /root/ktranfercheck  &&  /root/ktranfercheck/venv/bin/python /root/ktranfercheck/main.py >> /var/log/ktranfercheck.log',
        ssh_conn_id=cliente.replace(' ', '_')+'_server',
        conn_timeout=None,
        cmd_timeout=36000
    )    
    t5 = build_processing_tasks(client=cliente)
    t6 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})
    t1 >> t2 >> t5[0] 
    t5[-1] >> t6