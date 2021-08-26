from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from utils import build_transfer_tasks, check_transfer_tasks, build_processing_tasks, telegram_chat, dag_init
import pandas as pd
import config

CONN = 'sr_tendero_postgres'

REPO = 'sr-tendero-sql/sql/'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=5)
}
with DAG(
  dag_id="extraccion_completa_sr_tendero",
  description="Extraer información y procesarla",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval='5 6 * * *',
  catchup=False,
  tags=['sr-tendero', 'extraccion'],
) as dag:

    t1 = dag_init(dag.dag_id, CONN)

    tg1, task_log, task_log_names  = build_transfer_tasks(CONN, 'preprocessing')

    t1[-1] >> tg1

    # Revisión de errores durante la extracción
    t2 = BranchPythonOperator(
        task_id='Revision_de_errores',
        trigger_rule='all_done',
        python_callable=check_transfer_tasks,
        op_kwargs={'tasks': task_log, 'names': task_log_names}
    )
    tg1 >> t2 

    t3 = DummyOperator(
        task_id = 'Sin_errores_de_transferencia'
    )
    t4 = TelegramOperator(
        task_id = 'Notificar_errores_de_transferencia_a_soporte',
        telegram_conn_id='soporte2_telegram',
        chat_id= telegram_chat(),
        text = config.ALERTA_FALLA_SOPORTE
    )
    t5 = TelegramOperator(
        task_id = 'Notificar_errores_de_transferencia_a_cliente',
        telegram_conn_id='direccion_telegram',
        chat_id= telegram_chat(),
        text = config.ALERTA_FALLA_CLIENTE
    )

    t4 >> t5
    t2 >> Label("Sin errores") >> t3 
    t2 >> Label("Con errores") >> t4

    # Leer el listado de tareas de transformación
    tg2 = build_processing_tasks(CONN, REPO)

    t3 >> tg2[0]
    t5 >> tg2[0]

    t6 = PostgresOperator(
    task_id='Liberando_recursos', 
    postgres_conn_id=CONN,
    trigger_rule='all_done',
    sql='UPDATE dag_run SET available = True;')

    tg2[-1] >> t6

    t7 = TelegramOperator(
    task_id = 'Notificar_errores_de_procesamiento_a_soporte',
    telegram_conn_id='soporte2_telegram',
    chat_id= telegram_chat(),
    trigger_rule='all_failed',
    text = config.ALERTA_FALLA
    )
  
    t6 >> t7