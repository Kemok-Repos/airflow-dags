ALERTA_FALLA = """ALERTA: Error en el procesamiento de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}"""

ALERTA_FALLA_CLIENTE = """ALERTA: Error en la tranferencia de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n
\n
{{ task_instance.xcom_pull(task_ids='Revision_de_errores', key='mensaje_error_cliente') }}"""

ALERTA_FALLA_SOPORTE = """ALERTA: Error en la tranferencia de datos.\n
https://airflow.kemok.io/graph?dag_id={{ dag.dag_id }}\n
\n
{{ task_instance.xcom_pull(task_ids='Revision_de_errores', key='mensaje_error_equipo') }}"""

ACTIVE_DAG_RUNS = """
SELECT 
	COUNT(*)
FROM dag_run
WHERE state = 'running'
	AND dag_id = '{0}';
"""