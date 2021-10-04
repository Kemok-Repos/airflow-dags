SELECT 
    pid,
	datname AS database,
	usename AS username,
	client_addr::text AS ip,
	backend_start AT TIME ZONE 'CST'  AS start,
	NOW() AT TIME ZONE 'CST' AS stop,
	query,
	jsonb_build_object('wait_event_type', wait_event_type, 'wait_event', wait_event, 'backend_type', backend_type) AS properties
FROM pg_stat_activity 
WHERE state = 'active' 
    AND backend_start < NOW() - '1 hour'::interval;