from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from kemokrw.load_db import LoadDB
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError
from os import listdir, getcwd
import json
import re
import unidecode

PATH = getcwd() + '/dags/'


def app_environment():
    """ Retorna en que ambiente se encuentra la instancia. """
    try:
        env = Variable.get('APP_ENV')
    except KeyError:
        env = 'development'
    return


def get_airflow_connection(connection):
    """ Obtiene el connection string desde Airflow"""
    if connection:
        conn = BaseHook.get_connection(connection)
        if 'postgres' in connection:
            return LoadDB.built_connection_string(str(conn.login), str(conn.password), str(conn.host), str(conn.port),
                                                  str(conn.schema))
        elif 'sqlserver' in connection:
            return 'mssql+pymssql://{0}:{1}@{2}:{3}/{4}'.format(str(conn.login), str(conn.password), str(conn.host),
                                                                str(conn.port), str(conn.schema))


def get_task_name(x):
    """ Limpia los nombres de las tareas para hacerlas más entendibles y que cumplan con las restricciones. """
    x = x.replace(" ", "_")
    x = ''.join(e for e in x if e.isalpha() or e == '_' or e == '-' or e.isnumeric())
    x = unidecode.unidecode(x)
    x = re.sub('^\d+_+', '', x, count=1)
    x = re.sub('^t[mtr]\d+_+', '', x, count=1)
    x = re.sub('^vm\d+_+', '', x, count=1)
    x = re.sub('sql$', '', x, count=1)
    return x


def read_file(path, **kwargs):
    with open(path, 'r', encoding='utf-8') as file:
        query = file.read()
        return query.format(**kwargs)


def run_query(conn_id, query):
    conn_string = get_airflow_connection(conn_id)
    attempts = 0
    while attempts < 3:
        try:
            engine = create_engine(conn_string)
            connection = engine.connect()
            result = connection.execute(query)
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
    return result


def run_query_from_file(conn_id, path, **kwargs):
    query = read_file(path, **kwargs)
    run_query(conn_id, query)


def get_query_from_file(conn_id, path, keys, **kwargs):
    query = read_file(path, **kwargs)
    query_result = run_query(conn_id, query)
    result = []
    if query_result:
        for i in query_result:
            row = dict()
            for j, k in enumerate(i):
                row[keys[j]] = k
            result.append(row)
    return result


def get_params_from_query(conn_id, query):
    try:
        result = run_query(conn_id, query)
        result = result.fetchone()
        if result:
            result = result[0]
    except Exception as err:
        print(err)
        print('ERROR - No se pudo correr el query de obtención de parametros.')
        result = dict()
    result = result or dict()
    if type(result) is dict:
        return result
    else:
        return dict()

def get_task_from_file(url):
    if os.path.isfile(url):
        with open(url, 'r', encoding='utf-8') as task_file:
            task_dictionary = json.load(task_file)
            task_file.close()
    else:
        task_dictionary = dict()
    task_list = task_dictionary.get('tasks')
    if task_list and task_list is list:
        return task_list 
    else:
        return None
    

def insert_error_to_log(context):
    """ Función que inserta errores dentro del log para su notificación. """
    dag_object = context.get('dag')
    ti = context.get('ti')
    conn_id = ti.xcom_pull(key='conn_id', task_ids='Inicio.Revision_de_recursos.Configurar_corrida')
    params = {'dag_id': dag_object.dag_id, 'task_id': ti.task_id}
    run_query_from_file(conn_id, PATH + '/sql/_insert_error_to_log.sql', **params)


def check_slow_queries():
    connection_list = ["aquasistemas_postgres", "bac_personas_postgres", "bago_caricam_postgres", "bago_guatemala_postgres", "kemok_bi_postgres", "sr_tendero_postgres"]
    for conn_id in connection_list:
        result = run_query_from_file(conn_id, PATH+'sql/_slow_queries.sql')
        pid_list = []
        for slow_query in result:
            pid_list.append(slow_query[0])
        for pid in pid_list:
            print('Eliminar proceso '+str(pid))
            cancel = run_query(conn_id, "SELECT pg_cancel_backend('{0}')".format(str(pid)))
            for i in cancel:
                print(i[0])


def join_dicts(a, b):
    if not isinstance(a, dict):
        a = dict()
    if not isinstance(b, dict):
        b = dict()
    a = a or dict()
    b = b or dict()
    return {**a, **b}


if __name__ == '__main__':
    pass
