from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.edgemodifier import Label
from utils import get_query_from_file
from os import getcwd

PATH = getcwd() + '/dags/'


def get_current_runs(conn_id='airflow_postgres', path=PATH + '/sql/current_dag_runs.sql', **kwargs):
    """ Función que retorna descarta nuevas corridas de Airflow en base a la cantidad de corridas actuales. """
    dag_object = kwargs.get('dag')
    dag_runs_query = get_query_from_file(conn_id, path, ['conteo'], **{'dag_id': dag_object.dag_id})
    print(dag_runs_query[0])
    if dag_runs_query[0]['conteo'] <= 2:
        return 'Inicio.Revision_de_recursos.Encolar'
    else:
        return 'Inicio.Revision_de_recursos.Descartar'


def push_configuration(ti, conn_id):
    ti.xcom_push(key='conn_id', value=conn_id)


def dag_init(conn_id=None, client=None):
    """ Función que devuelve una cola para asegurar una solo corrida del DAG a la vez. """
    if client:
        client = client.replace(' ', '_').lower()
        conn_id = client+'_postgres'
    with TaskGroup(group_id='Inicio') as tg1:
        with TaskGroup(group_id='Revision_de_recursos') as tg2:
            dag_init_tasks = [BranchPythonOperator(
                task_id='Verificar_corridas_actuales',
                python_callable=get_current_runs
            ), SqlSensor(
                task_id='Encolar',
                conn_id=conn_id,
                sql='SELECT * FROM dag_run;',
                fail_on_empty=True,
                poke_interval=30,
                task_concurrency=1
            ), DummyOperator(
                task_id='Descartar'
            ), PostgresOperator(
                task_id='Reservar_recursos',
                postgres_conn_id=conn_id,
                sql='UPDATE dag_run SET available = False;'
            ), PythonOperator(
                task_id='Configurar_corrida',
                python_callable=push_configuration,
                op_kwargs={'conn_id': conn_id}
            )]

            dag_init_tasks[0] >> Label("Espacio en cola") >> dag_init_tasks[1]
            dag_init_tasks[0] >> Label("Cola llena") >> dag_init_tasks[2]
            dag_init_tasks[1] >> dag_init_tasks[3] >> dag_init_tasks[4]

        t3 = DummyOperator(
            task_id='Alistar_recursos',
            trigger_rule='one_success'
        )

        tg2 >> t3

    return tg1
