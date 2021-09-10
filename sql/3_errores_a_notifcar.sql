SELECT 
	dag_id,
	task_id
FROM dag_run_error
WHERE notified_at IS NULL
	AND dag_id = '{0}'
