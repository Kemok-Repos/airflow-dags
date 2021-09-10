SELECT 
	dag_id,
	task_id
FROM task_instance
WHERE dag_id = '{0}';