SELECT 
    pid 
FROM pg_stat_activity 
WHERE state = 'active' 
    AND backend_start < NOW() - '1 hour'::interval;