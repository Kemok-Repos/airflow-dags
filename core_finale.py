from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from core_notifications import message_tasks
from utils import get_query_from_file, run_query_from_file
from os import getcwd

PATH = getcwd() + '/dags/'


def get_current_errors(conn_id='airflow_postgres', path=PATH + '/sql/errors_in_current_dag_run.sql', **kwargs):
    """ Función que retorna la cantidad de errores en corrida actual """
    dag_object = kwargs.get('dag')
    execution_date = kwargs.get('execution_date')
    execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S.%f+00')
    dag_runs_query = get_query_from_file(conn_id, path, ['conteo'],
                                         **{'dag_id': dag_object.dag_id, 'execution_date': execution_date})
    print(dag_runs_query[0])
    if dag_runs_query[0]['conteo'] == 0:
        return 'Fin.Finalizar_proceso'
    else:
        return 'Fin.Manejar_errores'


def eliminate_errors_in_log(conn_id, path=PATH + '/sql/5_eliminar_errores.sql', **kwargs):
    """ Función que elimina los errores del DAG. """
    dag_object = kwargs.get('dag')
    run_query_from_file(conn_id, path, **{'dag_id': dag_object.dag_id})


def update_errors_as_notified(conn_id, path=PATH + '/sql/4_update_despues_de_notificar.sql', **kwargs):
    dag_object = kwargs.get('dag')
    run_query_from_file(conn_id, path, **{'dag_id': dag_object.dag_id})


def dag_finale(conn_id, **kwargs):
    """ Función que libera los recursos y notifica cualquier error. """
    with TaskGroup(group_id='Fin') as tfinal:
        with TaskGroup(group_id='Finalizando_procesos') as tg1:
            tasks = [PostgresOperator(
                task_id='Liberando_recursos',
                postgres_conn_id=conn_id,
                trigger_rule='none_skipped',
                sql='UPDATE dag_run SET available = True;'
            )]
            for i, j in enumerate(tasks):
                if i != 0:
                    tasks[i - 1] >> tasks[i]

        t2 = BranchPythonOperator(
            task_id='Revision_de_errores',
            python_callable=get_current_errors
        )

        tg1 >> t2

        t3 = DummyOperator(task_id='Finalizar_proceso')

        with TaskGroup(group_id='Manejar_proceso_exitoso') as tg4:
            tasks = [
                message_tasks(4, conn_id, **kwargs),  # Comunicar reparación de error
                DummyOperator(
                    task_id='Comunicar_a_usuarios'
                ), message_tasks(1, conn_id),  # Campaña a usuarios
                PythonOperator(
                    task_id='Limpiar_log',
                    python_callable=eliminate_errors_in_log,
                    op_kwargs={'conn_id': conn_id}
                )]
            for i, j in enumerate(tasks):
                if i != 0:
                    tasks[i - 1] >> tasks[i]

        t3 >> tg4

        t5 = DummyOperator(task_id='Manejar_errores')

        with TaskGroup(group_id='Manejar_proceso_con_errores') as tg6:
            tasks = [
                message_tasks(2, conn_id, **kwargs),
                DummyOperator(
                    task_id='Comunicar_a_cliente'
                ),
                message_tasks(3, conn_id, **kwargs),
                PythonOperator(
                    task_id='Actualizar_notificados',
                    python_callable=update_errors_as_notified,
                    op_kwargs={'conn_id': conn_id}
                )]
            for i, j in enumerate(tasks):
                if i != 0:
                    tasks[i - 1] >> tasks[i]

        t5 >> tg6

        t2 >> Label("Corrida sin errores") >> t3
        t2 >> Label("Corrida con errores") >> t5

    return tfinal
