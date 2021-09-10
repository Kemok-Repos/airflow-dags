from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from kemokrw.load_db import LoadDB
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError
from os import listdir, getcwd
import config
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
    except Exception:
        result = dict()
    result = result or dict()
    if type(result) is dict:
        return result
    else:
        return dict()


def insert_error_to_log(context):
    """ Función que inserta errores dentro del log para su notificación. """
    dag_object = context.get('dag')
    ti = context.get('ti')
    conn_id = ti.xcom_pull(key='conn_id', task_ids='Inicio.Configurar_corrida')
    params = {'dag_id': dag_object.dag_id, 'task_id': ti.task_id}
    run_query_from_file(conn_id, PATH + '/sql/0_insercion_de_error.sql', **params)


def revision_inactividad(conn_id, query_path='/opt/airflow/dags/sql/revision_inactividad.sql'):
    """ Método para obtener la inactividad a notificar"""
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
            executed_query = connection.execute(query)
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
    notif_tasks = []
    for i, j in enumerate(executed_query):
        mensaje = 'Alerta: {0} ha tenido {1} minutos de inactividad. Desde {2} hasta {3}'.format(str(j[0]), str(j[1]),
                                                                                                 str(j[2]), str(j[3]))
        notif_tasks.append(TelegramOperator(
            task_id='Alerta_{0}_{1}'.format(str(i), get_task_name(str(j[0]))),
            telegram_conn_id='direccion_telegram',
            text=mensaje
        ))
    if len(notif_tasks) == 0:
        notif_tasks.append(DummyOperator(task_id='Sin_alertas_de_inactividad'))
    return notif_tasks

if __name__ == '__main__':
    pass
