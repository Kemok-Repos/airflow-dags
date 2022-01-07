from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from utils import get_airflow_connection
from datetime import timedelta, datetime

def track_changes():
    src_engine = create_engine(get_airflow_connection('sr_tendero_sqlserver'))
    query = src_engine.execute('SELECT COUNT(*) AS conteo, SUM(montosubtotal) AS suma FROM InventarioCosto WHERE (year(fechaingreso)) >= (year(getdate()) -1);')
    result = query.first()

    insert_query = 'INSERT INTO revision_de_cambios (filas, suma) VALUES ({0}, {1});'.format(str(result[0]), str(result[1]))
    print(insert_query)

    dst_engine = create_engine(get_airflow_connection('sr_tendero_postgres'))
    dst_engine.execute(insert_query)

default_args = {
    'owner': 'airflow'
}
with DAG(
        dag_id='sr-tendero-monitor-de-base-de-datos',
        description="Revisa los cambios en la tabla de InventarioCosto",
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        schedule_interval='0,30 * * * *',
        catchup=False,
        tags=['revisión', 'sr tendero']
) as dag:

    t1 = PythonOperator(
        task_id="track_changes",
        python_callable=track_changes
    )
