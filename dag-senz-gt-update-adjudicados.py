import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.operators.ssh import SSHOperator

from utils import read_text

DAG_ID = 'senz-gt-update-adjudicados'


default_args = {
    'owner': 'airflow',
    'email': ['saul@kemok.io'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'parallel_tasks': 1,
    'conn_id': 'senz_gt_server_nano',
    'sla': timedelta(hours=1)
}
with DAG(
    dag_id=DAG_ID,
    description="",
    default_args=default_args,
    schedule_interval='10 0,6,10,13,15,18,21 * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz-gt'],
) as dag:
    ssh_id = default_args['conn_id']
    conn_id = ssh_id.split('_server')[0] + '_postgres'
    parallel_tasks = default_args['parallel_tasks']
    st = ['vigente', 'adjudicado', 'prescindido', 'desierto']
    sql = "select nog from finalizado where estatus = '%s';"


    def from_query(cur, sql: str):
        cur.execute(sql)
        return cur.fetchall()


    def get_data():
        order_by = 'order by fecha_actualizacion desc'
        conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
        cur, file_path = conn.cursor(), ['dags', 'senz-gt-sql', 'refresh_diario.sql']
        nogs = [x[0] for x in from_query(cur, f"select nog from finalizado where estatus = 'Adjudicado' {order_by}")]
        _, _, li = cur.close(), conn.close(), list(enumerate(nogs))
        return {idx: [t[1] for t in li if t[0] % parallel_tasks == idx] for idx in range(parallel_tasks)}


    t0, t1, tn = DummyOperator(task_id='start'), DummyOperator(task_id='parallelize'), DummyOperator(task_id='end')

    t0 >> t1
    for k, v in get_data().items():
        cmd = 'cd /opt/guatecompras && python3 sync_contests.py -nl %s -vb' % ','.join(v)
        t1 >> SSHOperator(task_id=f'run{k}', command=cmd, ssh_conn_id=ssh_id, conn_timeout=None, cmd_timeout=None) >> tn

