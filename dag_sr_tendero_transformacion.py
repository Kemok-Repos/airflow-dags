from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from transfer_manager import get_task_name
from datetime import datetime, timedelta
from os import listdir, getcwd

CONN = 'sr_tendero_postgres'

PATH = getcwd()

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
  dag_id="transformacion-sr-tendero",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#Proceso manual
  schedule_interval=None, #UTC
  max_active_runs=1,
  catchup=False,
  tags=['sr-tendero','transformacion'],
) as dag:
    # Leer el listado de tareas de transformación
    processing_task_groups = listdir(PATH+'/dags/sr-tendero-sql/sql') 
    processing_task_groups.sort()

    tg = []
    for i, group in enumerate(processing_task_groups):
        with TaskGroup(group_id=group) as task_group:
            t = []

            processing_tasks = listdir(PATH+'/dags/sr-tendero-sql/sql/'+group)
            processing_tasks.sort()

            for j, task in enumerate(processing_tasks):
                t.append(PostgresOperator(
                    task_id= get_task_name(task), 
                    trigger_rule='none_failed_or_skipped',
                    postgres_conn_id= CONN,
                    sql='sr-tendero-sql/sql/'+group+'/'+task))
                if j != 0:
                    t[j-1] >> t[j]
        tg.append(task_group)
        if i != 0:
            tg[i-1] >> tg[i]