UPDATE board_user
	SET url_params = REGEXP_REPLACE(url_params, '\d\d\d\d-\d\d', to_char(current_date, 'YYYY-MM'),'g')
WHERE url_params ~* '\d\d\d\d-\d\d' AND board_id IN (6,7,8,9,10,11,13,22,23,24);
