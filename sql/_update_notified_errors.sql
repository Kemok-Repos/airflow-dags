UPDATE dag_run_error
	SET notified_at = NOW()
	WHERE dag_id = '{dag_id}';