SELECT 
	CASE WHEN MAX(marca_temporal_de_carga) IS NOT NULL THEN True
		ELSE FALSE END nuevo_archivo
FROM archivo_gestor_general
WHERE marca_temporal_de_carga AT TIME ZONE 'UTC' NOT IN (SELECT cargas FROM auxiliar_cargas);