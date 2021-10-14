from airflow import DAG
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import timedelta, datetime

cliente = 'bac personas'
conn_id = cliente.replace(' ', '_')+'_postgres'

default_args = {
    'owner': 'airflow'
}
with DAG(
        dag_id='bac-personas-monitor-de-archivos',
        description="Revisa la últia actualización de los archivos de BAC",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        schedule_interval='0,5,10,15,20,25,30,35,40,45,50,55 * * * *',
        catchup=False,
        tags=['revisión', cliente]
) as dag:

    t1 = BranchSQLOperator(
        task_id='Revision-de-ulitma-carga',
        conn_id=conn_id,
        sql='sql/_check_for_new_file.sql',
        follow_task_ids_if_true='Actualizar_registros',
        follow_task_ids_if_false='Datos_a_la_fecha',
    )

    t2 = PostgresOperator(
        task_id='Actualizar_registros',
        postgres_conn_id=conn_id,
        sql='sql/_save_new_file.sql'
    )

    t3 = TelegramOperator( 
        task_id='Notificar_cliente',
        telegram_conn_id='kemok_telegram',
        chat_id='-339327210',
        text='Se ha detectado un nuevo archivo...iniciando procesamiento...'
    )

    t4 = TriggerDagRunOperator( 
        task_id='Iniciar_procesamiento',
        trigger_dag_id='bac-personas-procesamiento-completo-para-actualizacion-de-tableros'
    )

    t5 = DummyOperator( 
        task_id='Datos_a_la_fecha'
    )


    t1 >> t2 >> t3 >> t4
    t1 >> t5