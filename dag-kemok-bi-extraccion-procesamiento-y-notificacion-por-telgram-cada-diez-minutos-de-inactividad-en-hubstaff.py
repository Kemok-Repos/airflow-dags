from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
from core_transfer import TransferTasks
from core_notifications import NotificationTasks

cliente = 'kemok bi'

conn_id = cliente.replace(' ', '_')+'_postgres'

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}
with DAG(
        dag_id=cliente.replace(' ', '-')+'-extraccion-procesamiento-y-notificacion-por-telgram-cada-diez-minutos-de-inactividad-en-hubstaff',
        description="Extraer información y procesarla",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        schedule_interval='3,13,23,33,43,53 13-23,0-5 * * *',
        catchup=False,
        tags=['transferencia', 'procesamiento', cliente],
) as dag:

    t1 = TransferTasks(client=cliente, condition='hourly').task_groups()

    t2 = PostgresOperator(
            task_id='revision_de_inactividad',
            trigger_rule='all_done',
            postgres_conn_id=conn_id,
            sql="REFRESH MATERIALIZED VIEW vista_inactividad_reciente WITH DATA;",
        )
    nt1 = NotificationTasks(client=cliente)

    t3 = nt1.branch(5)
    t4 = nt1.tasks(5)

    nt2 = NotificationTasks(client=cliente)

    t5 = nt2.branch(6, trigger_rule='one_success')
    t6 = nt2.tasks(6)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
