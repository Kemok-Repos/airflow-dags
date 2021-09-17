from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta
from utils import insert_error_to_log
from core_initialize import dag_init
from core_finale import dag_finale
from pprint import pprint

cliente = 'kemok_bi'
conn_id = cliente.replace(' ', '_')+'_postgres'

def failure_func():
    raise AirflowException()
    # print('Holi')

default_args = {
    'owner': 'airflow'
}
with DAG(
        dag_id='prueba_'+cliente.replace(' ', '_'),
        description="Prueba de extracción",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
        tags=['transferencia', cliente]
) as dag:

    t0 = dag_init(conn_id)

    t1 = PythonOperator(
        task_id='tarea_kemok',
        python_callable=failure_func,
        on_failure_callback=insert_error_to_log
    )

    t2 = dag_finale(conn_id, **{'dag_id': dag.dag_id})

    t0 >> t1 >> t2