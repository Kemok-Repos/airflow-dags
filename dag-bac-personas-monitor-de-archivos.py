from airflow import DAG
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.bash_operator import BashOperator
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
        schedule_interval='0,10,20,30,40,50 * * * *',
        catchup=False,
        tags=['revisión', cliente]
) as dag:

    t1 = BranchSQLOperator(
        task_id='Revision_de_ulitma_carga',
        conn_id=conn_id,
        sql='sql/_check_for_new_file.sql',
        follow_task_ids_if_true='Actualizar_registros',
        follow_task_ids_if_false='Datos_a_la_fecha',
        # follow_task_ids_if_false='Actualizar_registros',
        # follow_task_ids_if_true='Datos_a_la_fecha',
    )

    t2 = PostgresOperator(
        task_id='Actualizar_registros',
        postgres_conn_id=conn_id,
        sql='sql/_save_new_file.sql'
    )

    t3 = TelegramOperator( 
        task_id='Notificar_cliente',
        telegram_conn_id='kemok_telegram',
        # chat_id = '674350416',
        chat_id='-339327210',
        text='Se ha detectado un nuevo archivo...iniciando procesamiento...'
    )

    t4 = BashOperator(
        task_id="Esperar",
        bash_command="sleep 15m"
    )

    t5 = TriggerDagRunOperator( 
        task_id='Iniciar_procesamiento',
        trigger_dag_id='bac-personas-procesamiento-completo-para-actualizacion-de-tableros',
        wait_for_completion=True
    )

    t6 = BranchSQLOperator(
        task_id='Revision_de_valores',
        conn_id=conn_id,
        sql='bac-personas-sql/tests/archivos_vs_tablero.sql',
        follow_task_ids_if_true='Notificar_final',
        follow_task_ids_if_false='Reiniciar_procesamiento',
    )

    t7 = TriggerDagRunOperator( 
        task_id='Reiniciar_procesamiento',
        trigger_dag_id='bac-personas-procesamiento-completo-para-actualizacion-de-tableros',
        wait_for_completion=True
    )

    t8 = BranchSQLOperator(
        task_id='Nueva_revision_de_valores',
        conn_id=conn_id,
        sql='bac-personas-sql/tests/archivos_vs_tablero.sql',
        follow_task_ids_if_true='Notificar_final',
        follow_task_ids_if_false='Notificar_error',
    )

    t9 = TelegramOperator( 
        task_id='Notificar_final',
        telegram_conn_id='kemok_telegram',
        # chat_id = '674350416',
        chat_id='-339327210',
        text='Se ha finalizado el procesamiento existosamente... datos verificados...'
    )

    t10 = TelegramOperator( 
        task_id='Notificar_error',
        telegram_conn_id='kemok_telegram',
        # chat_id = '674350416',
        chat_id='-339327210',
        text='Se ha detectado un error en el procesamiento. Nuestro equipo ya fue alertado al respecto.'
    )

    t11 = TelegramOperator( 
        task_id='Notificar_error_a_soporte',
        telegram_conn_id='kemok_telegram',
        # chat_id = '674350416',
        chat_id='-555467817',
        text='ERROR en BAC Personas: Discrepancia de datos entre archivos y tableros.'
    )

    t0 = DummyOperator( 
        task_id='Datos_a_la_fecha'
    )


    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t9
    t1 >> t0
    t6 >> t7 >> t8 >> t9 
    t8 >> t10 >> t11