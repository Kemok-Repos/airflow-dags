INSERT INTO dag_run_error (dag_id, task_id, created_at, updated_at)
	VALUES('{dag_id}', '{task_id}', NOW(), NOW())
	ON CONFLICT (dag_id, task_id)
	DO UPDATE SET updated_at = NOW();