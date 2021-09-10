from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from kemokrw.client_google import GoogleClient
from kemokrw.client_hubstaff import HubstaffClient
from kemokrw.client_teamwork import TeamworkClient
from kemokrw.client_zoho import ZohoClient
from kemokrw.extract_db import ExtractDB
from kemokrw.extract_file import ExtractFile
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.extract_hubstaff import ExtractHubstaff
from kemokrw.extract_teamwork import ExtractTeamwork
from kemokrw.extract_zoho import ExtractZoho
from kemokrw.load_db import LoadDB
from kemokrw.load_file import LoadFile
# from kemokrw.load_gsheet impor LoadGSheet
from kemokrw.transfer_basic import BasicTransfer
# from kemokrw.transfer_db_date
# from kemokrw.transfer_db_key
from utils import get_airflow_connection, get_task_name, insert_error_to_log, get_query_from_file, get_params_from_query
from pprint import pprint
from os import getcwd

PATH = getcwd() + '/dags/'


def get_transfer_list(conn_id, query_path=PATH + 'sql/transfer-query.sql', condition=''):
    """ Método para obtener el listado de extracción de un proyecto"""
    names = ['id', 'nombre', 'clase_transfer', 'clase_fuente', 'config_fuente', 'clase_destino', 'config_destino',
             'nombre_fuente', 'nombre_destino', 'query_fuente', 'query_destino']
    result = get_query_from_file(conn_id, query_path, names, **{'condition': condition})
    transfer_list = []
    for task in result:
        task['task_name'] = get_task_name(task['nombre'])

        # Nombre de grupo de tareas
        taskgroup_name = get_task_name(task['nombre_fuente'])
        task['taskgroup_name'] = taskgroup_name

        if task['query_fuente'].strip() != '':
            param = get_params_from_query(conn_id, task['query_fuente'])
            task['config_fuente'] = {**task['config_fuente'], **param}

        if task['query_destino'].strip() != '':
            param = get_params_from_query(conn_id, task['query_destino'])
            task['config_destino'] = {**task['config_destino'], **param}

        transfer_list.append(task)
    return transfer_list


def manage_transfer(ti, clase_transfer, clase_fuente, config_fuente, clase_destino, config_destino):
    """ Método para transferir datos """
    try:
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('                         Creando fuente - {0}'.format(clase_fuente))
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('')
        pprint(config_fuente)

        db_fuente = get_airflow_connection(config_fuente.get('connection'))

        if clase_fuente.lower() == 'extractdb':
            src = ExtractDB(db_fuente, config_fuente.get('table'), config_fuente.get('model'),
                            str(config_fuente.get('condition') or ''), str(config_fuente.get('order') or ''))
        elif clase_fuente.lower() == 'extractfile':
            src = ExtractFile(config_fuente.get('path'), config_fuente.get('model'), config_fuente.get('sheet'),
                              config_fuente.get('separator'), config_fuente.get('encoding'))
        elif clase_fuente.lower() == 'extractgsheet':
            src_client = GoogleClient(config_fuente.get('credential_file'), config_fuente.get('token_file'))
            src = ExtractGSheet(src_client, config_fuente.get('spreadsheetId'), config_fuente.get('range'),
                                config_fuente.get('model'))
        elif clase_fuente.lower() == 'extracthubstaff':
            conn = BaseHook.get_connection(config_fuente.get('connection'))
            src_client = HubstaffClient(config_fuente.get('path'), str(conn.password),
                                        config_fuente.get('organization'))
            src = ExtractHubstaff(src_client, config_fuente.get('url'), config_fuente.get('endpoint'),
                                  config_fuente.get('endpoint_type'), config_fuente.get('response_key'),
                                  config_fuente.get('model'), config_fuente.get('params'), config_fuente.get('by_list'))
        elif clase_fuente.lower() == 'extractteamwork':
            conn = BaseHook.get_connection(config_fuente.get('connection'))
            src_client = TeamworkClient(str(conn.password))
            src = ExtractTeamwork(src_client, config_fuente.get('url'), config_fuente.get('endpoint'),
                                  config_fuente.get('endpoint_type'), config_fuente.get('response_key'),
                                  config_fuente.get('model'), config_fuente.get('params'), config_fuente.get('by_list'))
        elif clase_fuente.lower() == 'extractzoho':
            src_client = ZohoClient(config_fuente.get('path'))
            src = ExtractZoho(src_client, config_fuente.get('url'), config_fuente.get('endpoint'),
                              config_fuente.get('endpoint_type'), config_fuente.get('response_key'),
                              config_fuente.get('model'), config_fuente.get('params'), config_fuente.get('by_list'))
        elif clase_fuente.lower() == 'eltools':
            pass
        else:
            print(clase_fuente + ' no es una clase de fuente valida.')

        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('                         Creando destino - {0}'.format(clase_destino))
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('')
        pprint(config_destino)
        db_carga = get_airflow_connection(config_destino.get('connection'))

        if clase_destino.lower() == 'loaddb':
            dst = LoadDB(db_carga, config_destino.get('table'), config_destino.get('model'),
                         str(config_destino.get('condition') or ''), str(config_destino.get('order') or ''))
        elif clase_destino.lower() == 'loadfile':
            dst = LoadFile(config_destino.get('path'), config_destino.get('sheet'), config_destino.get('model'))
        elif clase_destino.lower() == 'eltools':
            pass
        else:
            print(clase_destino + ' no es una clase de destino valida.')
        
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('                         Transfiriendo - {0}'.format(clase_transfer))
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('')
        if clase_transfer.lower() == 'basictransfer':
            trf = BasicTransfer(src, dst)
            trf.transfer(2)
        elif clase_transfer.lower() == 'eltools':
            pass
        else:
            print(clase_transfer + ' no es una clase de transferencia valida.')

    except Exception as err:
        print(err)
        raise err


def build_transfer_tasks(connection_id, condition='', query_path='', kwargs_para_fuente=dict(),
                         kwargs_para_destino=dict()):
    """ Función que crea y agrupa las tareas de transferencia """
    if condition != '':
        condition = "WHERE (maestro_de_transferencias.propiedades->>'{0}')::boolean".format(condition)
    # Obtener listado de tareas de transferencia
    if query_path == '':
        transfer_tasks = get_transfer_list(connection_id, condition=condition)
    else:
        transfer_tasks = get_transfer_list(connection_id, condition=condition, query_path=query_path)

    kwargs_para_fuente = kwargs_para_fuente or dict()
    kwargs_para_destino = kwargs_para_destino or dict()
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

                    task['config_fuente'] = {**task['config_fuente'], **kwargs_para_fuente}
                    task['config_destino'] = {**task['config_destino'], **kwargs_para_destino}

                    t.append(PythonOperator(
                        task_id=task['task_name'],
                        python_callable=manage_transfer,
                        op_kwargs={'clase_transfer': task['clase_transfer'],
                                   'clase_fuente': task['clase_fuente'],
                                   'config_fuente': task['config_fuente'],
                                   'clase_destino': task['clase_destino'],
                                   'config_destino': task['config_destino']},
                        on_failure_callback=insert_error_to_log))
        tg.append(task_group)
    if len(tg) == 0:
        with TaskGroup(group_id='Extraer_informacion') as task_group:
            t = DummyOperator(task_id='Sin_fuentes_a_transferir')
        tg.append(task_group)

    return tg
