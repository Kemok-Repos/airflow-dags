from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from utils import get_task_name, insert_error_to_log
from os import getcwd, listdir

PATH = getcwd() + '/dags/'


def build_processing_tasks(connection_id=None, repo=None, client=None):
    if not repo and not connection_id and client:
        connection_id = client.replace(' ', '_').lower()+'_postgres'
        repo = client.replace(' ', '-').lower()+'-sql/sql/'
    # Obtener listado de tareas de transformacion
    processing_task_groups = listdir(PATH + repo)
    processing_task_groups.sort()

    tg = []
    for i, group in enumerate(processing_task_groups):
        with TaskGroup(group_id='Procesar_' + get_task_name(group)) as task_group:
            t = []

            processing_tasks = listdir(PATH + repo + '/' + group)
            processing_tasks.sort()

            for j, task in enumerate(processing_tasks):
                t.append(PostgresOperator(
                    task_id=get_task_name(task),
                    trigger_rule='none_failed_or_skipped',
                    postgres_conn_id=connection_id,
                    sql=repo + '/' + group + '/' + task,
                    on_failure_callback=insert_error_to_log))
                if j != 0:
                    t[j - 1] >> t[j]
        tg.append(task_group)
        if i != 0:
            tg[i - 1] >> tg[i]
    return tg
