from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from transfer_manager import get_transfer_list, manage_transfer
import pandas as pd


def branc_func(ti, tasks, names):
    print(tasks)
    errors = ti.xcom_pull(key='error', task_ids=tasks)
    print(errors)
    errors_to_notify = dict()
    for i, j in enumerate(errors):
        if j:
            task = tasks[i]
            name = names[i]
            errors_to_notify[task] = {'error': j, 'name': name}
    print(errors_to_notify)
    if len(errors_to_notify) == 0:
        return 'Sin_errores'
    else:
        error_message = ''
        for i,j in enumerate(errors_to_notify):
            error_message += '{0}). Error en {1}\n\n'.format(str(i), errors_to_notify[j]['name'])
        ti.xcom_push(key='error_message', value=error_message)
        return 'Notificar_errores'

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
  dag_id="extraccion-diaria-bago-guatemala",
  description="Extraer información del centro de mando",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval='0 6 * * *',
  catchup=False,
  tags=['bago-guatemala', 'extraccion'],
) as dag:
    transfers = get_transfer_list('bago_guatemala_postgres')
    t1 = []
    tasks = []
    task_names = []
    for j in transfers:
        t1.append(PythonOperator(
            task_id=j['task_name'],
            python_callable=manage_transfer,
            op_kwargs={'clase_transfer': j['clase_transfer'],
                        'clase_fuente': j['clase_fuente'],
                        'config_fuente': j['config_fuente'],
                        'clase_destino': j['clase_destino'],
                        'config_destino': j['config_destino']}))
        tasks.append(j['task_name'])
        task_names.append(j['nombre'])
    
    t2 = BranchPythonOperator(
        task_id='Revision_de_errores',
        trigger_rule='all_done',
        python_callable=branc_func,
        op_kwargs={'tasks': tasks, 'names': task_names}
    )
    t3 = DummyOperator(
        task_id = 'Sin_errores'
    )
    t4 = TelegramOperator(
        task_id = 'Notificar_errores',
        telegram_conn_id='direccion_telegram',
        text = """ALERTA: Problemas en la tranferencia de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n
\n
{{ task_instance.xcom_pull(task_ids='Revision_de_errores', key='error_message') }}"""
    )
    

    t1 >> t2 
    t2 >> Label("Sin errores") >> t3
    t2 >> Label("Con errores") >> t4
