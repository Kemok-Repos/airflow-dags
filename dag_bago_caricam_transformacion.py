from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from kontact.client import KontactClient
from transfer_manager import get_task_name
from datetime import datetime, timedelta
from os import listdir, getcwd

CONN = 'bago_caricam_postgres'

PATH = getcwd()

ALERTA_FALLA_SOPORTE = """ALERTA: Error en el procesamiento de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n"""

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=120)    
}
with DAG(
  dag_id="transformacion_bago_caricam",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso manual
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['bago_caricam','transformacion'],
) as dag:
    # Leer el listado de tareas de transformación
    processing_task_groups = listdir(PATH+'/dags/bago-caricam-sql/sql') 
    processing_task_groups.sort()

    tg = []
    for i, group in enumerate(processing_task_groups):
        with TaskGroup(group_id=group) as task_group:
            t = []

            processing_tasks = listdir(PATH+'/dags/bago-caricam-sql/sql/'+group)
            processing_tasks.sort()

            for j, task in enumerate(processing_tasks):
                t.append(PostgresOperator(
                    task_id= get_task_name(task), 
                    trigger_rule='none_failed_or_skipped',
                    postgres_conn_id= CONN,
                    sql='bago-caricam-sql/sql/'+group+'/'+task))
                if j != 0:
                    t[j-1] >> t[j]
        tg.append(task_group)
        if i != 0:
            tg[i-1] >> tg[i]

    t1 = TelegramOperator(
    task_id = 'Notificar_errores_a_soporte',
    telegram_conn_id='soporte2_telegram',
    trigger_rule='all_failed',
    text = ALERTA_FALLA_SOPORTE
    )

    tg[-1] >> t1

