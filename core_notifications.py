from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from kontact.client import KontactClient
from utils import get_query_from_file, get_task_name, get_params_from_query
from jinja2 import Template
from pprint import pprint
from os import getcwd

PATH = getcwd() + '/dags/'


def send_campaign(kontact_key, campaign_id):
    client = KontactClient(kontact_key)
    result = client.send_campaign(int(campaign_id))
    pprint(result)


def message_tasks(id_task, conn_id, path=PATH+'/sql/get_notifications.sql', trigger_rule='all_success',
                  wo_name='Sin_notificaciones_{}', **kwargs):
    notifications = []
    keys = ['task_id', 'type', 'address', 'message_template', 'query_template', 'schedule', 'params']
    tasks = get_query_from_file(conn_id, path, keys, **{'id_task': str(id_task)})
    for task in tasks:
        params1 = task['params'] or dict()
        params2 = kwargs or dict()
        params = {**params1, **params2}
        if task['type'] == 'Kontact':
            if 'kontact_key' in params.keys():
                notifications.append(PythonOperator(
                    task_id=get_task_name(task['task_id']),
                    python_callable=send_campaign,
                    trigger_rule=trigger_rule,
                    op_kwargs={'kontact_key': params['kontact_key'],
                               'campaign_id': task['address']},
                    retries=0
                ))
        elif task['type'] == 'Telegram':
            params3 = dict()
            if task['query_template'].strip() != '':
                params3 = get_params_from_query(conn_id, task['query_template'].format(**params))
            if params3 != dict():
                params = {**params, **params3}
                message = Template(task['message_template'], trim_blocks=True, lstrip_blocks=True).render(**params)
                notifications.append(TelegramOperator(
                    task_id=get_task_name(task['task_id']),
                    telegram_conn_id='kemok_telegram',
                    chat_id=task['address'],
                    trigger_rule=trigger_rule,
                    text=message
                    ))
    if len(notifications) == 0:
        notifications.append(DummyOperator(
                task_id=wo_name.format(str(id_task)),
                trigger_rule=trigger_rule
            ))
    return notifications


if __name__ == '__main__':
    conn_id = 'kemok_bi_postgres'
    kwargs = {'dag_id': 'prueba_kemok_bi'}
    a = message_tasks(2, conn_id, path=getcwd()+'/sql/get_notifications.sql', **kwargs)
