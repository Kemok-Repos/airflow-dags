SELECT
	dag_notification_addresses.id_task AS message_id,
	dag_notification_tasks.task_id AS message,
	dag_notification_tasks.branching_query,
	dag_notification_tasks.task_id||' a '||dag_addresses.name AS task_id,
	dag_notification_tasks.type,
	dag_addresses.address,
	dag_notification_tasks.message_template,
	dag_notification_tasks.query_template,
	dag_addresses.params
FROM dag_notification_addresses
JOIN dag_notification_tasks ON dag_notification_addresses.id_task = dag_notification_tasks.id
JOIN dag_addresses ON dag_notification_addresses.id_address = dag_addresses.id;