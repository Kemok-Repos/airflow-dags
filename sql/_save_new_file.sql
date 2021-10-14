INSERT INTO auxiliar_cargas
SELECT 
	MAX(marca_temporal_de_carga AT TIME ZONE 'UTC')  AS hora
FROM archivo_gestor_general
WHERE marca_temporal_de_carga AT TIME ZONE 'UTC' NOT IN (SELECT cargas FROM auxiliar_cargas)
HAVING MAX(marca_temporal_de_carga AT TIME ZONE 'UTC') IS NOT NULL;