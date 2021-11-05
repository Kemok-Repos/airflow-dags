from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from core_notifications import NotificationTasks
from utils import get_query_from_file, run_query_from_file
from os import getcwd

PATH = getcwd() + '/dags/'


def get_current_errors(conn_id='airflow_postgres', path=PATH + '/sql/_errors_in_current_dag_run.sql', **kwargs):
    """ Función que retorna la cantidad de errores en corrida actual """
    dag_object = kwargs.get('dag')
    run_id = kwargs.get('run_id')
    dag_runs_query = get_query_from_file(conn_id, path, ['conteo'],
                                         **{'dag_id': dag_object.dag_id, 'run_id': run_id})
    print(dag_runs_query[0])
    if dag_runs_query[0]['conteo'] == 0:
        return 'Fin.Finalizar_proceso'
    else:
        return 'Fin.Manejar_errores'


def eliminate_errors_in_log(conn_id, path=PATH + '/sql/_clean_log.sql', **kwargs):
    """ Función que elimina los errores del DAG. """
    dag_object = kwargs.get('dag')
    run_query_from_file(conn_id, path, **{'dag_id': dag_object.dag_id})


def update_errors_as_notified(conn_id, path=PATH + '/sql/_update_notified_errors.sql', **kwargs):
    dag_object = kwargs.get('dag')
    run_query_from_file(conn_id, path, **{'dag_id': dag_object.dag_id})


def dag_finale(conn_id=None, client=None, **kwargs):
    """ Función que libera los recursos y notifica cualquier error. """
    if client:
        client = client.replace(' ', '_').lower()
        conn_id = client+'_postgres'
    with TaskGroup(group_id='Fin') as tfinal:
        with TaskGroup(group_id='Finalizando_procesos') as tg1:
            tasks = [PostgresOperator(
                task_id='Liberando_recursos',
                postgres_conn_id=conn_id,
                trigger_rule='all_done',
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
            nt1 = NotificationTasks(conn_id=conn_id, **kwargs)

            tasks = [
                nt1.branch(4, task_group='Fin.Manejar_proceso_exitoso.'),
                nt1.tasks(4),
                nt1.tasks(1, trigger_rule='one_success')[0],
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
            nt2 = NotificationTasks(conn_id=conn_id, **kwargs)
            nt3 = NotificationTasks(conn_id=conn_id, **kwargs)
            tasks = [
                nt2.branch(2, task_group='Fin.Manejar_proceso_con_errores.'),
                nt2.tasks(2),
                nt3.branch(3, task_group='Fin.Manejar_proceso_con_errores.', trigger_rule='one_success'),
                nt3.tasks(3),
                PythonOperator(
                    task_id='Actualizar_notificados',
                    python_callable=update_errors_as_notified,
                    op_kwargs={'conn_id': conn_id},
                    trigger_rule='one_success'
                )]
            for i, j in enumerate(tasks):
                if i != 0:
                    tasks[i - 1] >> tasks[i]

        t5 >> tg6

        t2 >> Label("Corrida sin errores") >> t3
        t2 >> Label("Corrida con errores") >> t5

    return tfinal
