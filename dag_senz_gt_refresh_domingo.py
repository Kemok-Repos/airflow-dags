from airflow import DAG
from core_finale import dag_finale
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils import read_text

DAG_ID = 'senz-guatemala-refresh-domingo'


default_args = {
    'owner': 'airflow',
    'email': ['saul@kemok.io'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'conn_id': 'senz_gt_postgres',
    'pool': 'Senz Gt',
    'sla': timedelta(hours=1)
}
with DAG(
    dag_id=DAG_ID,
    description="",
    default_args=default_args,
    schedule_interval='0 23 * * 0',
    start_date=datetime(2022, 3, 2),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=350),
    tags=['senz-guatemala'],
) as dag:
    conn_id = default_args['conn_id']
    sql = 'refresh materialized view %s with data;'

    def get_views():
        conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
        cur, file_path = conn.cursor(), ['dags', 'senz-gt-sql', 'refresh_domingo.sql']
        cur.execute(read_text(file_path))
        views = [sql % x[0] for x in cur.fetchall()]
        cur.close(), conn.close()
        return views

    t0 = DummyOperator(task_id='get_views')
    t1 = PostgresOperator(task_id='refresh_views', sql=get_views(), postgres_conn_id=conn_id, autocommit=True)
    end = DummyOperator(task_id='end')
    # end = dag_finale(conn_id=conn_id, dag_id='end')

    t0 >> t1 >> end
