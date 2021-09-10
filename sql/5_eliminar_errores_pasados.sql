DELETE FROM dag_run_error
WHERE notified_at IS NOT NULL 
	AND notified_at < NOW() - '24 hours'::interval;