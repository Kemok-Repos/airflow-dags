from os import listdir, getcwd
from os.path import isfile, join

PATH = getcwd()

HEADER = """from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

CONN = '{1}_postgres'

default_args = {{
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  False,
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(minutes=10)    
}}
with DAG(
  dag_id="transformacion-{0}",
  description="Transforma la informacion cruda para el uso de las aplicaciones",
  default_args=default_args,
  start_date=days_ago(1),
#{3}
  schedule_interval='{2}', #UTC
  catchup=False,
  tags=['{0}','transformacion'],
) as dag:
"""

TASK_GROUP = """    # Start Task Group Definition
    path = '{2}/'
    with TaskGroup(group_id='{0}') as tg{1}:\n"""
TASK = """        {1} = PostgresOperator(task_id="{2}", postgres_conn_id=CONN,
                                 sql=path+"{0}")\n"""                                 

if __name__ == '__main__':
    for k in listdir(PATH):
        if k[-4:] == '-sql':

            if k == 'aquasistemas-sql':
                horario = '0 7 * * *'
                comentario = 'Proceso normal a las 5:00 AM'
            elif k == 'bac-personas-sql':
                horario = '0 6 * * *'
                comentario = 'Proceso asincrono'
            elif k == 'bago-caricam-sql':
                horario = '0 6 * * *'
                comentario = 'Proceso asincrono'
            elif k == 'bago-guatemala-sql':
                horario = '30 5 * * *'
                comentario = 'Proceso normal a las 3:00 AM'
            elif k == 'sr-tendero-bi-sql':
                horario = '0 3 * * *'
                comentario = 'Proceso normal a las 3:00 AM'


            name = k[:-4]
            conn = name.replace('-', '_')
            file = HEADER.format(name, conn, horario, comentario)

            tg = 1
            orden_tg = []
            grupos = listdir(PATH+'/'+k+'/sql')
            grupos.sort()
            
            for x, i in enumerate(grupos):
                file += TASK_GROUP.format(i, tg, k+'/sql/'+i)
                orden_tg.append('tg'+str(tg))
                tg += 1
                queries = listdir(PATH+'/'+k+'/sql'+'/'+i)
                queries.sort()
                orden = []
                for y, j in enumerate(queries):
                    a = j.split('.')[0]
                    b = a.split('_')[0]
                    c = a.replace(b+'_', "")
                    c = c.replace(' ', '')
                    c = c.replace('-', '')
                    task = TASK.format(j, chr(x+97)+str(y), c)
                    file += task
                    orden.append(chr(x+97)+str(y))
                file += '\n'
                z = 0
                while z < len(orden):
                    if z == 0:
                        file += '        ' + ' >> '.join(orden[:10]) + '\n'
                        z += 10
                    else:
                        file += '        ' + ' >> '.join(orden[z-1:z+9]) + '\n'
                        z += 9

                file += "   # End Task Group definition"
                file += '\n\n'
            file += '    '+' >> '.join(orden_tg)

            #f = open("D:/GitHub Repos/astronomer/dags/dag_transformacion_"+conn+".py", "w")
            f = open(getcwd()+"/dag_"+conn+"_transformacion.py", "w")
            f.write(file)
            f.close()

