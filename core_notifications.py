from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from custom_operator import KontactOperator, DinamicTelegramOperator
from utils import get_query_from_file, get_task_name, run_query, join_dicts
from pprint import pprint
from os import getcwd, path
import json

PATH = getcwd() + '/dags/'

class NotificationTasks:
    def __init__(self, client=None, conn_id=None, trigger_rule=None, message_id=None, **kwargs):
        if client:
            client = client.replace(' ', '_').lower()
            self.conn_id = client+'_postgres'
        if conn_id:
            self.conn_id = conn_id
            client = conn_id.replace('_postgres', '')
        self.trigger_rule = trigger_rule
        self.message_id = message_id        
        self.params = kwargs
        self.task_group = None
        self.configuration_path='/opt/airflow/dags/config/notifications_{0}.json'.format(client)
        self.query_path='/opt/airflow/dags/sql/_notifications.sql'

        if not path.exists(self.configuration_path):
            self.get_notification_tasks()

    def get_notification_tasks(self):
        tasks = dict()
        keys = ['message_id','message', 'branching_query','task_id', 'type', 'address', 'message_template', 'query_template', 'params']
        messages = get_query_from_file(self.conn_id, self.query_path, keys)
        ids = []
        for message in messages:
            ids.append(message['message_id'])
        ids = set(ids)
        for id in ids:
            tasks[id] = {"message": None, "tasks": [], "branching_query": None}
        for message in messages:
            tasks[message['message_id']]['message'] = message['message']
            tasks[message['message_id']]['branching_query'] = message['branching_query']
            task = message.copy()
            task.pop('message_id')
            task.pop('message')
            task.pop('branching_query')
            tasks[message['message_id']]['tasks'].append(task)
        with open(self.configuration_path, 'w', encoding='utf-8') as file:
            json.dump(tasks, file)


    def read_notification_tasks(self, message_id):
        with open(self.configuration_path, 'r', encoding='utf-8') as file:
            tasks = json.load(file)
            tasks = tasks[str(message_id)]
            return tasks    


    def tasks(self, message_id, trigger_rule='all_success', **kwargs):
        tasks = self.read_notification_tasks(message_id)
        notifications = []
        for task in tasks['tasks']:
            params = join_dicts(task['params'], kwargs)
            params = join_dicts(params, self.params)
            if task['type'] == 'Kontact':
                notifications.append(KontactOperator(
                    task_id=get_task_name(task['task_id']),
                    kontact_key=params['kontact_key'],
                    campaign_id=task['address'],
                    trigger_rule=trigger_rule,
                    retries=0
                ))
            elif task['type'] == 'Telegram':
                notifications.append(DinamicTelegramOperator(
                    task_id=get_task_name(task['task_id']),
                    chat_id=task['address'],
                    text=task['message_template'],
                    params_conn_id=self.conn_id,
                    params_query=task['query_template'].format(**params),
                    trigger_rule=trigger_rule,
                    retries=0,
                    additional_params=params
                ))
        branch = tasks['branching_query'] or '' 
        if branch.strip() != '':
            notifications.append(DummyOperator(task_id='Sin_'+get_task_name(tasks['message'].lower())))
        return notifications

    def branch(self, message_id, trigger_rule='all_success', task_group=None):
        self.message_id=message_id
        self.task_group= task_group or ''
        tasks = self.read_notification_tasks(message_id)
        return BranchPythonOperator(
            task_id='Seleccionando_'+get_task_name(tasks['message'].lower()),
            python_callable=self.branch_callable,
            trigger_rule=trigger_rule,
            retries=0
        )

    def branch_callable(self, **kwargs):
        dag_object = kwargs.get('dag')
        self.params['dag_id']=dag_object.dag_id
        self.params['message_id']=str(self.message_id) 
        tasks = self.read_notification_tasks(self.message_id)
        try:
            choices = run_query(self.conn_id, tasks['branching_query'].format(**self.params))
        except Exception as err:
            print(err)
            choices = []
        
        next_tasks = []
        for i in choices:
            next_tasks.append(i)
        if len(next_tasks) == 0:
            return self.task_group+'Sin_'+get_task_name(tasks['message'].lower())
        else:
            branches = []
            for i in next_tasks:
                branches.append(self.task_group+get_task_name(i[0]))
            return branches

if __name__ == '__main__':
    nt1 = NotificationTasks(conn_id='kemok_bi_postgres', **{'dag_id': 'abc'})
    a = nt1.tasks(2)