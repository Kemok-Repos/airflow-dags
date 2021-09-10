SELECT 
	id,
	nombre,
	clase_transfer,
	clase_fuente,
	config_fuente, 
	clase_destino,
	config_destino,
	nombre_origen,
	nombre_destino,
	'SELECT 1;' AS query_origen,
	'SELECT 1;' AS query_destino
FROM __transfers__ AS maestro_de_transferencias 
{condition}
ORDER BY 1;