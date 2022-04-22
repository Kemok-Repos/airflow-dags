from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from core_notifications import NotificationTasks
from datetime import timedelta, datetime

cliente = 'sr tendero'

default_args = {
    'owner': 'airflow',
    'email': ['wilmer@kemok.io'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'sla': timedelta(minutes=10)
}
with DAG(
    dag_id=cliente.replace(' ', '-')+'-alerta-de-pedidos-no-recibidos',
    description="Envio de alerta de pedidos no recibidos.",
    default_args=default_args,
    schedule_interval='0 14 * * 1-5',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sr tendero', 'comunicaciÃ³n', 'kontact'],
) as dag:

    t1 = PostgresOperator (
        task_id='Actualizar-tabla-notificaciones',
        postgres_conn_id='sr_tendero_postgres',
        sql="sr-tendero-sql/actualizar-alerta-ordenes-no-entregadas.sql"
    )

    t2 = PostgresOperator (
        task_id='Actualizar-vista-notificaciones',
        postgres_conn_id='sr_tendero_postgres',
        sql='REFRESH MATERIALIZED VIEW __notificaciones__ WITH DATA;'
    )

    t3 = NotificationTasks(client=cliente).tasks(8)

    t1 >> t2 >> t3