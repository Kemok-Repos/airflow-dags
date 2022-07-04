import json
from datetime import date, timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from utils import read_text

DAG_ID = 'senz-gt-search-yesterday-contests'


default_args = {
    'owner': 'airflow',
    'email': ['saul@kemok.io'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'conn_id': 'senz_gt_server_nano',
    'pool': 'guatecompras',
    'sla': timedelta(hours=1)
}
with DAG(
    dag_id=DAG_ID,
    description="",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz-gt'],
) as dag:
    conn_id = default_args['conn_id']
    sd, ed = (date.today() - timedelta(days=1)).isoformat(), date.today().isoformat()
    cmd = f'cd /opt/guatecompras && python3 search_contests.py -sd {sd} -ed  {ed} -vb'
    t0, tn = DummyOperator(task_id='start'), DummyOperator(task_id='end')
    t1 = SSHOperator(task_id='run', command=cmd, ssh_conn_id=conn_id, conn_timeout=None, cmd_timeout=1800)

    t0 >> t1 >> tn
