UPDATE board_user
	SET url_params = 'mes_y_a_o='||to_char(current_date, 'YYYY-MM')
	WHERE url_params IS NULL AND board_id IN (6,7,8,9,24);

UPDATE board_user
	SET url_params = 'temporada=2022-01'||to_char(current_date, 'YYYY-MM')
	WHERE url_params IS NULL AND board_id IN (11,13,22,23);