SELECT 
	COUNT(*)
FROM dag_run
WHERE state = 'running'
	AND dag_id = '{dag_id}';