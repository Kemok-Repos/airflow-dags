WITH modelos AS (
SELECT 
	maestro_de_modelos.id,
	maestro_de_modelos.modelo,
	maestro_de_origen.origen,
	COALESCE(maestro_de_modelos.configuracion||maestro_de_origen.propiedades, maestro_de_modelos.configuracion, maestro_de_origen.propiedades)  AS configuracion,
	COALESCE(maestro_de_modelos.query, 'SELECT 1;') AS query
FROM maestro_de_modelos
LEFT JOIN maestro_de_origen ON maestro_de_modelos.id_origen = maestro_de_origen.id
)
SELECT 
	maestro_de_transferencias.id,
	'Transferir '||modelo_fuente.modelo||'  a  '||modelo_destino.modelo AS nombre,
	maestro_de_transferencias.clase_transfer,
	maestro_de_transferencias.clase_fuente,
	modelo_fuente.configuracion AS config_fuente, 
	maestro_de_transferencias.clase_destino,
	modelo_destino.configuracion AS config_destino,
	'Extraer '||modelo_fuente.origen AS nombre_origen,
	'Cargar '||modelo_destino.origen AS nombre_destino,
	modelo_fuente.query AS query_fuente,
	modelo_destino.query AS query_destino
FROM maestro_de_transferencias 
LEFT JOIN modelos AS modelo_fuente ON id_fuente = modelo_fuente.id
LEFT JOIN modelos AS modelo_destino ON id_destino = modelo_destino.id
{condition}
ORDER BY 1;