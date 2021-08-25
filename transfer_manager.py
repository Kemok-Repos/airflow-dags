from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Variable
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
#from kemokrw.load_gsheet impor LoadGSheet
from kemokrw.transfer_basic import BasicTransfer
#from kemokrw.transfer_db_date
#from kemokrw.transfer_db_key
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError
from os import listdir, getcwd
from airflow.hooks.base import BaseHook
import unidecode

PATH = getcwd()+'/dags/'

def dev_key():
    if bool(Variable.get("DEV")):
        return '674350416'
    else:
        return None

def get_airflow_connection(connection):
    if connection:
        conn = BaseHook.get_connection(connection)
        return LoadDB.built_connection_string(str(conn.login), str(conn.password), str(conn.host), str(conn.port),
                                              str(conn.schema))

def get_task_name(x):
    x = x.replace(" ", "_")
    x = ''.join(e for e in x if e.isalpha() or e == '_' or e == '-' or e.isnumeric())
    x = unidecode.unidecode(x)
    return x

def get_transfer_list(conn_id, query_path=PATH+'sql/transfer-query.sql', condition=''):
    """ Método para obtener el listado de extracción de un proyecto"""
    try:
        file = open(query_path, "r")
        query = file.read()
    except FileNotFoundError as err:
        print('No se encuentra el archivo de query.')
        raise err

    db = get_airflow_connection(conn_id)
    engine = create_engine(db)
    attempts = 0
    while attempts < 3:
        try:
            connection = engine.connect()
            config_query = connection.execute(query.format(condition))
            connection.close()
            break
        except OperationalError as err:
            attempts += 1
            if attempts == 3:
                raise err
        except DatabaseError as err:
            attempts += 1
            if attempts == 3:
                raise err
    names = ['id', 'nombre', 'clase_transfer', 'clase_fuente', 'config_fuente', 'clase_destino', 'config_destino', 'nombre_fuente', 'nombre_destino']
    transfer_list = []
    for i in config_query:
        config = dict()
        for j, k in enumerate(i):
            config[names[j]] = k
        # Nombre de tarea
        task_name = get_task_name(config['nombre'])
        task_name = '{0}_{1}'.format(config['id'], task_name)
        config['task_name'] = task_name

        # Nombre de grupo de tareas
        taskgroup_name = get_task_name(config['nombre_fuente'])
        config['taskgroup_name'] = taskgroup_name

        transfer_list.append(config)

    return transfer_list


def manage_transfer(ti, clase_transfer, clase_fuente, config_fuente, clase_destino, config_destino):
    """ Método para transferir datos """
    try:
        # Creación de objeto fuente
        print('Creando objecto de extracción')
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
            src_client = HubstaffClient(config_fuente.get('path'), str(conn.password), config_fuente.get('organization'))
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
        else:
            print(clase_fuente + ' no es una clase de fuente valida.')

        # Creación de objeto destino
        print('Creando objeto de carga')
        db_carga = get_airflow_connection(config_destino.get('connection'))

        if clase_destino.lower() == 'loaddb':
            dst = LoadDB(db_carga, config_destino.get('table'), config_destino.get('model'),
                         str(config_destino.get('condition') or ''), str(config_destino.get('order') or ''))
        elif clase_destino.lower() == 'loadfile':
            dst = LoadFile(config_destino.get('path'), config_destino.get('sheet'), config_destino.get('model'))
        else:
            print(transfer['clase_destino'] + ' no es una clase de destino valida.')


        # Transferencia
        print('Transfiriendo datos')
        if clase_transfer.lower() == 'basictransfer':
            trf = BasicTransfer(src, dst)
            trf.transfer(2)
        else:
            print(clase_transfer + ' no es una clase de transferencia valida.')

    except Exception as err:
        print(err)
        ti.xcom_push(key='error', value=str(err))
        raise err


def revision_de_tareas_de_trasferencia(ti, tasks, names, branch_no='Sin_errores', branch_yes='Notificar_errores_a_soporte'):
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
        return branch_no

    # Escoge la proxima tarea si hay errores
    else:
        ti.xcom_push(key='mensaje_error_cliente', value=mensaje_error_cliente)
        ti.xcom_push(key='mensaje_error_equipo', value=mensaje_error_equipo)
        return branch_yes

def build_transfer_tasks(connection_id, condition=''):
    if condition != '':
        condition = "WHERE (maestro_de_transferencias.propiedades->>'{0}')::boolean".format(condition)
    # Obtener listado de tareas de transferencia
    transfer_tasks = get_transfer_list(connection_id, condition=condition)

    # Obtener los grupos de tareas en base al origen
    transfer_task_groups = []
    for task in transfer_tasks:
        transfer_task_groups.append(task['taskgroup_name'])
    transfer_task_groups = set(transfer_task_groups)

    # Crear un listado de grupos de tareas
    task_log = []
    task_log_names = []
    tg = []
    for group in transfer_task_groups:
        with TaskGroup(group_id=group) as task_group:
            # Crear un listado de tareas por cada grupo
            t = []
            for task in transfer_tasks:
                if task['taskgroup_name'] == group:
                    t.append(PythonOperator(
                        task_id=task['task_name'],
                        python_callable=manage_transfer,
                        op_kwargs={'clase_transfer': task['clase_transfer'],
                                    'clase_fuente': task['clase_fuente'],
                                    'config_fuente': task['config_fuente'],
                                    'clase_destino': task['clase_destino'],
                                    'config_destino': task['config_destino']}))
                    
                    task_log.append(group+'.'+task['task_name'])
                    task_log_names.append(task['nombre'])

        tg.append(task_group)        
    return tg, task_log, task_log_names

def build_processing_tasks(connection_id, repo):
    # Obetener listado de tareas de transformacion
    processing_task_groups = listdir(PATH+repo)
    processing_task_groups.sort()

    tg = []
    for i, group in enumerate(processing_task_groups):
        with TaskGroup(group_id=group) as task_group:
            t = []

            processing_tasks = listdir(PATH+repo+'/'+group)
            processing_tasks.sort()

            for j, task in enumerate(processing_tasks):
                t.append(PostgresOperator(
                    task_id= get_task_name(task), 
                    trigger_rule='none_failed_or_skipped',
                    postgres_conn_id=connection_id,
                    sql=repo+'/'+group+'/'+task))
                if j != 0:
                    t[j-1] >> t[j]
        tg.append(task_group)
        if i != 0:
            tg[i-1] >> tg[i]
    return tg

def check_transfer_tasks(ti, tasks, names):
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
        return 'Sin_errores_de_transferencia'

    # Escoge la proxima tarea si hay errores
    else:
        ti.xcom_push(key='mensaje_error_cliente', value=mensaje_error_cliente)
        ti.xcom_push(key='mensaje_error_equipo', value=mensaje_error_equipo)
        return 'Notificar_errores_de_transferencia_a_soporte'


if __name__ == '__main__':
    transfers = get_transfer_list('sr_tendero_postgres')


    for transfer in transfers:
        print(transfer['task_name'])

        result = manage_transfer(None, transfer['clase_transfer'], transfer['clase_fuente'], transfer['config_fuente'],
                                 transfer['clase_destino'], transfer['config_destino'])

        print(result)
