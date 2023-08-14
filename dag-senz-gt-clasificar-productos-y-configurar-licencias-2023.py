from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from core_transfer import TransferTasks
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'email': ['antonio@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=60)
}
with DAG(
    dag_id='senz-gt-manejo-de-productos-y-demos-2023',
    description="Réplica y configuración de productos de kemok.",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    #dagrun_timeout=timedelta(minutes=350),
    tags=['senz', 'ssh'],
) as dag:

    t1 = PostgresOperator (
        task_id='Truncar-tabla-g2s',
        postgres_conn_id='senz_gt_postgres',
        sql="senz-gt-replica-sql/truncate_g2s_asignacion_npgs.sql"
    )

    t2 = TransferTasks(client='senz gt', condition='test').task_groups()

    t3 = TransferTasks(client='senz pa', condition='test').task_groups()

    t4 = SSHOperator(
        task_id='clasificar-productos-y-configurar-licencias',
        command='cd /opt/productos-guatecompras-senz-gt-2023/ ; python main.py',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )

    t5 = SSHOperator(
        task_id='aplicar-cambios-en-metabase',
        command='cd /opt/replicador.kemok.app ; source .env/bin/activate ; python main.py ; deactivate ; ',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )

    t6 = SSHOperator(
        task_id='eliminar-licencias',
        command='cd /opt/productos-guatecompras-senz-gt-2023/ ; python main.py',
        ssh_conn_id='elt_server',
        conn_timeout=None,
        cmd_timeout=36000
    )

    t1 >> t2
    t2 >> t4
    t3 >> t4
    t4 >> t5 >> t6