from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=60)
}
with DAG(
    dag_id='senz-manejo-de-productos-y-demos',
    description="Réplica y configuración de productos de kemok.",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    #dagrun_timeout=timedelta(minutes=350),
    tags=['senz', 'ssh'],
) as dag:

    t1 = SSHOperator(
        task_id='clasificar-productos-y-configurar-licencias',
        command='cd /opt/productos-guatecompras/ ; python main.py',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )


    t2 = SSHOperator(
        task_id='aplicar-cambios-en-metabase',
        command='cd /opt/metamanager/ ; python main.py',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )

    t3 = SSHOperator(
        task_id='eliminar-licencias',
        command='cd /opt/productos-guatecompras/ ; python main.py',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )

    t1 >> t2 >> t3