from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from transfer_manager import get_transfer_list, manage_transfer
import pandas as pd


ALERTA_FALLA_CLIENTE = """ALERTA: Error en la tranferencia de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n
\n
{{ task_instance.xcom_pull(task_ids='Revision_de_errores', key='mensaje_error_cliente') }}"""

ALERTA_FALLA_SOPORTE = """ALERTA: Error en la tranferencia de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n
\n
{{ task_instance.xcom_pull(task_ids='Revision_de_errores', key='mensaje_error_equipo') }}"""


def revision_de_tareas_de_trasferencia(ti, tasks, names):
    # Leer los errores de cada una de las tareas de extracción
    errors = ti.xcom_pull(key='error', task_ids=tasks)
    mensaje_error_cliente = ''
    mensaje_error_equipo = ''
    n_errores = 0
    for i, j in enumerate(errors):
        if j:
            mensaje_error_cliente += '{0}). Error en {1}\n\n'.format(str(i), names[i])
            mensaje_error_equipo += '{0}). Error en {1}\n\ntask_id: {2}\nError:  {3}\n\n'.format(str(i), names[i], tasks[i], str(j))
            n_errores += 1
    # Escoge la proxima tarea si no hay errores
    if n_errores == 0:
        return 'Sin_errores'

    # Escoge la proxima tarea si hay errores
    else:
        ti.xcom_push(key='mensaje_error_cliente', value=mensaje_error_cliente)
        ti.xcom_push(key='mensaje_error_equipo', value=mensaje_error_equipo)
        return 'Notificar_errores_a_soporte'

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
  dag_id="extraccion-diaria-sr-tendero",
  description="Extraer información del centro de mando",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval='5 6 * * *',
  catchup=False,
  tags=['sr-tendero', 'extraccion'],
) as dag:

    # Leer el listado de tareas de extracción
    transfer_tasks = get_transfer_list('sr_tendero_postgres')

    # Organizar las tareas de extracción en grupos de tareas paralelas
    transfer_task_groups = []
    for task in transfer_tasks:
        transfer_task_groups.append(task['taskgroup_name'])
    transfer_task_groups = set(transfer_task_groups)
    task_log = []
    task_log_names = []
    tg1 = []
    for group in transfer_task_groups:
        with TaskGroup(group_id=group) as task_group:
            t1 = []
            for task in transfer_tasks:
                if task['taskgroup_name'] == group:
                    t1.append(PythonOperator(
                        task_id=task['task_name'],
                        python_callable=manage_transfer,
                        op_kwargs={'clase_transfer': task['clase_transfer'],
                                    'clase_fuente': task['clase_fuente'],
                                    'config_fuente': task['config_fuente'],
                                    'clase_destino': task['clase_destino'],
                                    'config_destino': task['config_destino']}))
                    
                    task_log.append(group+'.'+task['task_name'])
                    task_log_names.append(task['nombre'])

        tg1.append(task_group)

    # Revisión de errores durante la extracción
    t2 = BranchPythonOperator(
        task_id='Revision_de_errores',
        trigger_rule='all_done',
        python_callable=revision_de_tareas_de_trasferencia,
        op_kwargs={'tasks': task_log, 'names': task_log_names}
    )
    tg1 >> t2 

    t3 = DummyOperator(
        task_id = 'Sin_errores'
    )
    t4 = TelegramOperator(
        task_id = 'Notificar_errores_a_soporte',
        telegram_conn_id='soporte2_telegram',
        text = ALERTA_FALLA_SOPORTE
    )
    # t5 = TelegramOperator(
    #     task_id = 'Notificar_errores_a_cliente',
    #     telegram_conn_id='direccion_telegram',
    #     text = ALERTA_FALLA_CLIENTE
    # )

    # t4 >> t5

    t2 >> Label("Sin errores") >> t3
    t2 >> Label("Con errores") >> t4
