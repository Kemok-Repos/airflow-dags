from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from core_notifications import NotificationTasks
from core_transfer import TransferTasks
from utils import check_slow_queries

n1 = NotificationTasks(client='aquasistemas')
n2 = NotificationTasks(client='bac personas')
n3 = NotificationTasks(client='bago guatemala')
n4 = NotificationTasks(client='bago caricam')
n5 = NotificationTasks(client='kemok bi')
n6 = NotificationTasks(client='sr tendero')
n7 = NotificationTasks(client='kat')

t1 = TransferTasks(client='aquasistemas')
t2 = TransferTasks(client='bac personas')
t3 = TransferTasks(client='bago guatemala')
t4 = TransferTasks(client='bago caricam')
t5 = TransferTasks(client='kemok bi')
t6 = TransferTasks(client='sr tendero')
t7 = TransferTasks(client='senz pa')

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_failure': False,
    'sla': timedelta(minutes=10)
}

with DAG(
    dag_id='mantenimiento-actualizar-configuracion-de-transferencias-y-notificaciones',
    description="Refrescar configuración de notificaciones y transferencias",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 5 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['mantenimiento'],
) as dag:
    tn1 = PythonOperator(
        task_id='Refrescar_notificaciones_aquasistemas',
        python_callable=n1.get_notification_tasks,
    )
    tn2 = PythonOperator(
        task_id='Refrescar_notificaciones_bac_personas',
        python_callable=n2.get_notification_tasks,
    )
    tn3 = PythonOperator(
        task_id='Refrescar_notificaciones_bago_guatemala',
        python_callable=n3.get_notification_tasks,
    )
    tn4 = PythonOperator(
        task_id='Refrescar_notificaciones_bago_caricam',
        python_callable=n4.get_notification_tasks,
    )
    tn5 = PythonOperator(
        task_id='Refrescar_notificaciones_kemok_bi',
        python_callable=n5.get_notification_tasks,
    )
    tn6 = PythonOperator(
        task_id='Refrescar_notificaciones_sr_tendero',
        python_callable=n6.get_notification_tasks,
    )
    tn7 = PythonOperator(
        task_id='Refrescar_notificaciones_kat',
        python_callable=n7.get_notification_tasks,
    )
    tt1 = PythonOperator(
        task_id='Refrescar_transferencias_aquasistemas',
        python_callable=t1.get_transfer_tasks,
    )
    tt2 = PythonOperator(
        task_id='Refrescar_transferencias_bac_personas',
        python_callable=t2.get_transfer_tasks,
    )
    tt3 = PythonOperator(
        task_id='Refrescar_transferencias_bago_guatemala',
        python_callable=t3.get_transfer_tasks,
    )
    tt4 = PythonOperator(
        task_id='Refrescar_transferencias_bago_caricam',
        python_callable=t4.get_transfer_tasks,
    )
    tt5 = PythonOperator(
        task_id='Refrescar_transferencias_kemok_bi',
        python_callable=t5.get_transfer_tasks,
    )
    tt6 = PythonOperator(
        task_id='Refrescar_transferencias_sr_tendero',
        python_callable=t6.get_transfer_tasks,
    )
    tt7 = PythonOperator(
        task_id='Refrescar_transferencias_senz_pa',
        python_callable=t7.get_transfer_tasks,
    )
    tt8 = PythonOperator(
        task_id='Refrescar_transferencias_senz_gt',
        python_callable=t7.get_transfer_tasks,
    )