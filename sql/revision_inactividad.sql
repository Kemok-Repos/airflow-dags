WITH base AS (
SELECT 
	starts_at AS inicio,
	lag(starts_at) OVER (PARTITION BY user_id ORDER BY starts_at DESC) AS fin,
	user_id,
	overall=0 AS inactivo,
	tracked/60 AS registro,
	row_number() OVER (PARTITION BY user_id ORDER BY starts_at) as seq,
	row_number() OVER (PARTITION BY user_id, overall=0 ORDER BY starts_at) as seqalt
FROM hs_activities
WHERE time_slot::date = current_date
)
SELECT 
	hs_users.name AS usuario,
	SUM(registro)::int AS minutos,
	MIN(inicio) AS inicio,
	MIN(inicio) + make_interval(mins => SUM(registro)::int) AS fin
FROM base
LEFT JOIN hs_users ON base.user_id = hs_users.id
WHERE inactivo
GROUP BY 1, (seq - seqalt)
HAVING MIN(inicio) + make_interval(mins => SUM(registro)::int) >= NOW() - '20 minutes'::interval
	AND SUM(registro) > 20
ORDER BY 3