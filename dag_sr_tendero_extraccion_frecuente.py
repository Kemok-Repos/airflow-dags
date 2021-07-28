from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from kemokrw.extract_db import ExtractDB
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
from sqlalchemy import create_engine
import pandas as pd

con_string = "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}"
conn = BaseHook.get_connection("sr_tendero_pos_postgres")
DB1 = con_string.format(str(conn.login), str(conn.password), str(conn.host), str(conn.port), str(conn.schema))
conn = BaseHook.get_connection("sr_tendero_postgres")
DB2 = con_string.format(str(conn.login), str(conn.password), str(conn.host), str(conn.port), str(conn.schema))
QUERY = """SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public'     
AND table_name = '{0}';"""

tablas = ['store_turns', 'stock_movements', 'stock_movements_detail', 'sells']


def get_model(db, table, include_columns=None, exclude_columns=None):
    print(db)
    model = dict()
    engine = create_engine(db)
    connection = engine.connect()
    data = pd.read_sql(sql=QUERY.format(table), con=connection)
    connection.close()
    data.sort_values(['ordinal_position'], ascending=True, ignore_index=True, inplace=True)

    if include_columns is not None and exclude_columns is None:
        data = data[data['column_name'].isin(include_columns)]
    elif include_columns is None and exclude_columns is not None:
        data = data[~data['column_name'].isin(exclude_columns)]
    elif include_columns is not None and exclude_columns is not None:
        columns = [x for x in include_columns if x not in exclude_columns]
        data = data[data['column_name'].isin(columns)]
    data.reset_index(inplace=True, drop=True)
    for index, row in data.iterrows():
        model['col'+str(index+1)] = {'name': row['column_name'], 'type': row['data_type']}
    return model


def transfer_table(table):
    print('Transfering {0} table to erp_pos_{0}'.format(table))
    model1 = get_model(DB1, table)
    model2 = get_model(DB2, 'erp_pos_'+table)
    src = ExtractDB(DB1, table, model1)
    dst = LoadDB(DB2, 'erp_pos_'+table, model2)
    src.get_data()
    trf = BasicTransfer(src, dst)
    trf.transfer(2)


default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=5)
}
with DAG(
  dag_id="extraccion-frecuente-sr-tendero",
  description="Extrae la información necesaria para el reporte diario",
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval='0,30 13-23,0-5 * * *',
  catchup=False,
  tags=['sr-tendero','extraccion'],
) as dag:
    t = []
    for i, j in enumerate(tablas):
        t.append(PythonOperator(
            task_id='transfer_'+j,
            python_callable=transfer_table,
            op_kwargs={'table':j}))
        if i != 0:
            t[i-1] >> t[i]
