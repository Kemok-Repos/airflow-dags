SELECT 
	COUNT(*)
FROM task_instance
WHERE state = 'failed'
	AND dag_id = '{dag_id}' AND execution_date = '{execution_date}';