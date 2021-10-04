from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from utils import insert_error_to_log
from core_initialize import dag_init
from core_finale import dag_finale

cliente = 'kemok_bi'
conn_id = cliente.replace(' ', '_')+'_postgres'

def failure_func():
    raise AirflowException()
    # print('Holi')

default_args = {
    'owner': 'airflow'
}
with DAG(
        dag_id='prueba-manejo-de-inicio-y-fin',
        description="Prueba de extracción",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
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