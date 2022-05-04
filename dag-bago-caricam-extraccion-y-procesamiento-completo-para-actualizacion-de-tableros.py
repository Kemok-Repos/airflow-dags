from airflow import DAG
from datetime import timedelta, datetime
from core_initialize import dag_init
from core_transfer import TransferTasks
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from core_processing import build_processing_tasks
from core_finale import dag_finale

cliente = 'bago caricam'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=90)
}
with DAG(
    dag_id=cliente.replace(' ', '-')+'-extraccion-y-procesamiento-completo-para-actualizacion-de-tableros',
    description="Extraer información y procesarla",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='30 8 * * *',
    catchup=False,
    tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = dag_init(client=cliente)
    t2 = TransferTasks(client=cliente, condition='preprocessing').task_groups()
    t3 = build_processing_tasks(client=cliente)
    t4 = BranchSQLOperator(
        task_id='Revision-de-datos',
        sql="SELECT bool_and(resultado) AND date_trunc('month', current_date) NOT IN (SELECT fecha FROM test_fecha) FROM tests;",
#       follow_task_ids_if_true='Activar-ventana-cambio',
        follow_task_ids_if_false='Data-no-lista',
        conn_id='bago_caricam_postgres'
    )
    t6 = PostgresOperator (
        task_id='Actualizar-test-fecha',
        postgres_conn_id='bago_caricam_postgres',
        sql="bago-caricam-sql/actualizar-test-fecha.sql"
    )
    t7 = TriggerDagRunOperator(    
        task_id="Actualizar-datos-en-app",
        trigger_dag_id="bago-caricam-actualizacion-de-datos-en-aplicacion-de-proyecciones-de-venta",
        conf={"message": "Hello World"},
    )
    t8 = DummyOperator(task_id='Data-no-lista')
    t9 = dag_finale(client=cliente, **{'dag_id': dag.dag_id})

    t1 >> t2 >> t3[0] 
    t3[-1] >> t4 >> t6 >> t7 >> t9 
    t4 >> t8 >> t9
