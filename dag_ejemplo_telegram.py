#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Telegram operator.
"""

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'email': ['kevin@kemok.io'],
    'email_on_sucess':  True,
    'email_on_failure': True,
    'email_on_retry':   True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=10)    
}

dag = DAG(
    dag_id='ejemplo_telegram',
    description="Un ejemplo de como configurar un DAG y como utilizar el operador de Telegram.",
    default_args=default_args,
    schedule_interval='@once',
    start_date=days_ago(1),
    #end_date=  #Fecha de finalización
    catchup = False, #El DAGs se pone 'al dia' y corre n veces que no se ha corrido desde la fecha de inicio hasta el momento en base al intervalo de 
    tags=['ejemplo','comunicación'],
    
    #concurrency=1 # Número máximo de tareas a ejecutarse al mismo tiempo   
    #max_active_runs=1 # Número máximo de DAGs a ejecutarse al mismo tiempo
    #sla_miss_callback= función a llamar en caso falle el SLA 
    #on_failure_callback=
    #on_sucess_callback=  
)

send_message_telegram_task = TelegramOperator(
    task_id='send_message_telegram',
    telegram_conn_id='producto_telegram',
    text='{{ dag_run.conf["message"] }}',
    dag=dag
)
