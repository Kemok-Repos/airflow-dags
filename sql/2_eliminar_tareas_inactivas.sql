DELETE FROM dag_run_error
WHERE dag_id = {0} AND task_id = {1};