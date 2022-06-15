from airflow import DAG
from functools import reduce
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils import read_text

DAG_ID = 'senz-gt-refresh-diario'


default_args = {
    'owner': 'airflow',
    'email': ['saul@kemok.io'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'parallel_tasks': 3,
    'conn_id': 'senz_gt_mart_postgres',
    'pool': 'senz_gt',
    'sla': timedelta(hours=1)
}
with DAG(
    dag_id=DAG_ID,
    description="",
    default_args=default_args,
    schedule_interval='0 1 * * *',
    start_date=datetime(2022, 3, 2),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz-guatemala'],
) as dag:
    conn_id = default_args['conn_id']
    parallel_tasks = default_args['parallel_tasks']
    sql = 'refresh materialized view %s with data;'

    def get_data():
        conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
        cur, file_path = conn.cursor(), ['dags', 'senz-gt-sql', 'refresh_diario.sql']
        cur.execute(read_text(file_path))
        data = cur.fetchall()
        nits = {x[0]: x for x in data}
        cur.close(), conn.close()
        return {n: list(map(lambda x: sql % x[1], filter(lambda x: x[0] == n, data))) for n in nits}

    t0 = PostgresOperator(task_id='refresh_kpis', sql=sql % 'kpis_resumen_por_cliente', postgres_conn_id=conn_id, autocommit=True)
    t1 = PostgresOperator(task_id='refresh_gv_npg', sql=sql % 'gv_npg', postgres_conn_id=conn_id, autocommit=True)
    parallelize = DummyOperator(task_id='parallelize')
    end = DummyOperator(task_id='end')

    t0 >> t1 >> parallelize

    refresh_data = get_data()
    keys = list(refresh_data.keys())
    queues = {f'q{x+1}': reduce(lambda a, b: {**a, keys[b]: refresh_data[keys[b]]}, range(x, len(keys), parallel_tasks), {}) for x in range(parallel_tasks)}

    for dic in queues.values():
        s = parallelize
        for key, li in dic.items():
            t = PostgresOperator(task_id=f"refresh_views_{key}", sql=li, postgres_conn_id=conn_id, autocommit=True)
            s >> t
            s = t
        s >> end
