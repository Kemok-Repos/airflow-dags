from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from custom_operator import TransferOperator
from utils import get_airflow_connection, get_task_name, insert_error_to_log, get_query_from_file, get_params_from_query
from pprint import pprint
from os import getcwd, path
import json

PATH = getcwd() + '/dags/'

class TransferTasks:
    def __init__(self, client=None, conn_id=None, condition=None):
        if client:
            client = client.replace(' ', '_').lower()
            self.conn_id = client+'_postgres'
        if conn_id:
            self.conn_id = conn_id
            client = conn_id.replace('_postgres', '')
        self.condition = condition or 'test'
        self.configuration_path='/opt/airflow/dags/config/transfers_{0}.json'.format(client)
        self.query_path='/opt/airflow/dags/sql/_transfers.sql'
        self.client = client
        if not path.exists(self.configuration_path):
            self.get_transfer_tasks()

    def get_transfer_tasks(self, **kwargs):
        tasks = []
        names = ['id', 'nombre', 'clase_transfer', 'clase_fuente', 'config_fuente', 'clase_destino', 'config_destino',
                 'nombre_fuente', 'nombre_destino', 'query_fuente', 'query_destino', 'propiedades']
        result = get_query_from_file(self.conn_id, self.query_path, names, **kwargs)
        for task in result:
            task['task_name'] = get_task_name(task['nombre'])

            # Nombre de grupo de tareas
            taskgroup_name = get_task_name(task['nombre_fuente'])
            task['taskgroup_name'] = taskgroup_name+'_de_'+self.client.replace(' ', '_').lower()

            tasks.append(task)
        with open(self.configuration_path, 'w', encoding='utf-8') as file:
            json.dump(tasks, file)

    def read_transfer_tasks(self, condition):
        with open(self.configuration_path, 'r', encoding='utf-8') as file:
            tasks = json.load(file)
            transfer_task = []
            for task in tasks:
                task['propiedades'] = task['propiedades'] or dict()
                conditions = task['propiedades'].keys()
                if condition in conditions:
                    transfer_task.append(task)
            return transfer_task   

    def task_groups(self, kwargs_fuente=dict(), kwargs_destino=dict()):
        transfer_tasks = self.read_transfer_tasks(self.condition)

        kwargs_fuente = kwargs_fuente or dict()
        kwargs_destino = kwargs_destino or dict()
        # Obtener los grupos de tareas en base al origen
        transfer_task_groups = []
        for task in transfer_tasks:
            transfer_task_groups.append(task['taskgroup_name'])
        transfer_task_groups = set(transfer_task_groups)

        # Crear un listado de grupos de tareas
        tg = []
        for group in transfer_task_groups:
            with TaskGroup(group_id=group) as task_group:
                # Crear un listado de tareas por cada grupo
                t = []
                for task in transfer_tasks:
                    if task['taskgroup_name'] == group:

                        task['config_fuente'] = {**task['config_fuente'], **kwargs_fuente}
                        task['config_destino'] = {**task['config_destino'], **kwargs_destino}

                        t.append(TransferOperator(
                            task_id=task['task_name'],
                            conn_id=self.conn_id,
                            clase_transfer=task['clase_transfer'],
                            clase_fuente=task['clase_fuente'],
                            config_fuente=task['config_fuente'],
                            query_fuente=task['query_fuente'],
                            clase_destino=task['clase_destino'],
                            config_destino=task['config_destino'],
                            query_destino=task['query_destino'],
                            on_failure_callback=insert_error_to_log
                        ))
            tg.append(task_group)
        if len(tg) == 0:
            with TaskGroup(group_id='Extraer_informacion'+'_de_'+self.client.replace(' ', '_').lower()) as task_group:
                t = DummyOperator(task_id='Sin_fuentes_a_transferir'+'_a_'+self.client.replace(' ', '_').lower())
            tg.append(task_group)
            
        return tg
