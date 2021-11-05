SELECT 
	COUNT(*)
FROM task_instance
WHERE state = 'failed'
	AND dag_id = '{dag_id}' AND run_id = '{run_id}';